from __future__ import annotations

import json
import posixpath
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import uuid4

import asyncpg

from services.chunker import chunk_text, store_chunks
from services.document_links import (
    MARKDOWNISH_FILE_TYPES,
    build_document_location,
    rebase_relative_markdown_links,
    rewrite_markdown_links_to_target,
)
from services.kb_access import is_wiki_path

RETENTION_DAYS = 7
_WIKI_MARKDOWN_TYPES = {"md", "txt", "note"}
_WIKI_PROTECTED = {("/wiki/", "overview.md"), ("/wiki/", "log.md")}
_INTERNAL_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


@dataclass(frozen=True)
class ReleasePage:
    page_key: str
    path: str
    filename: str
    title: str | None
    content: str
    tags: list[str]
    sort_order: int

    @property
    def full_path(self) -> str:
        return build_document_location(self.path, self.filename)


@dataclass(frozen=True)
class ReleaseValidation:
    ok: bool
    report: dict[str, Any]
    errors: list[str]


def _normalize_path(path: str | None) -> str:
    normalized = (path or "/").strip()
    if not normalized:
        return "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if not normalized.endswith("/"):
        normalized += "/"
    return re.sub(r"/+", "/", normalized)


def _full_path(path: str, filename: str) -> str:
    return build_document_location(_normalize_path(path), filename)


def _split_full_path(full_path: str) -> tuple[str, str]:
    clean = full_path.lstrip("/")
    if "/" in clean:
        return _normalize_path(clean.rsplit("/", 1)[0]), clean.rsplit("/", 1)[1]
    return "/", clean


def _is_wiki_markdown_doc(row: dict[str, Any]) -> bool:
    return is_wiki_path(row.get("path")) and (row.get("file_type") in _WIKI_MARKDOWN_TYPES)


def _normalize_coverage_unit(text: str) -> str:
    normalized = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text or "")
    normalized = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", normalized)
    normalized = re.sub(r"^#+\s*", "", normalized.strip(), flags=re.MULTILINE)
    normalized = re.sub(r"\s+", " ", normalized.strip().lower())
    return normalized.strip(" -\t")


def _coverage_units(text: str) -> set[str]:
    units: set[str] = set()
    for block in re.split(r"\n\s*\n", text or ""):
        normalized = _normalize_coverage_unit(block)
        if len(normalized) >= 24:
            units.add(normalized)
    return units


def _duplicate_signature(page: ReleasePage) -> str:
    content = _normalize_coverage_unit(page.content)
    return f"{_normalize_coverage_unit(page.title or page.filename)}::{content[:600]}"


async def ensure_initial_wiki_release(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    *,
    actor: str = "backfill",
) -> str:
    active = await conn.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        knowledge_base_id,
    )
    if active:
        return active

    release_id = str(uuid4())
    await conn.execute(
        "INSERT INTO wiki_releases (id, knowledge_base_id, status, created_by, published_at) VALUES ($1::uuid, $2::uuid, 'published', $3, now())",
        release_id,
        knowledge_base_id,
        actor,
    )

    rows = await conn.fetch(
        "SELECT id::text AS id, path, filename, title, COALESCE(content, '') AS content, COALESCE(tags, '{}'::text[]) AS tags, COALESCE(sort_order, 0) AS sort_order "
        "FROM documents WHERE knowledge_base_id = $1::uuid AND NOT archived AND path LIKE '/wiki/%%' AND file_type = ANY($2::text[]) "
        "ORDER BY path, filename",
        knowledge_base_id,
        list(_WIKI_MARKDOWN_TYPES),
    )
    if rows:
        await conn.executemany(
            "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::text[], $8)",
            [
                (
                    release_id,
                    row["id"],
                    _normalize_path(row["path"]),
                    row["filename"],
                    row["title"],
                    row["content"] or "",
                    list(row["tags"] or []),
                    row["sort_order"] or 0,
                )
                for row in rows
            ],
        )
    await conn.execute(
        "UPDATE knowledge_base_settings SET active_wiki_release_id = $1::uuid, updated_at = now() WHERE knowledge_base_id = $2::uuid",
        release_id,
        knowledge_base_id,
    )
    return release_id


async def create_draft_release(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    *,
    created_by: str,
    created_by_run_id: str | None = None,
) -> tuple[str, str]:
    active_release_id = await ensure_initial_wiki_release(conn, knowledge_base_id)
    release_id = str(uuid4())
    await conn.execute(
        "INSERT INTO wiki_releases (id, knowledge_base_id, status, base_release_id, created_by, created_by_run_id) VALUES ($1::uuid, $2::uuid, 'draft', $3::uuid, $4, $5::uuid)",
        release_id,
        knowledge_base_id,
        active_release_id,
        created_by,
        created_by_run_id,
    )
    await conn.execute(
        "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) "
        "SELECT $1::uuid, page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $2::uuid",
        release_id,
        active_release_id,
    )
    await conn.execute(
        "INSERT INTO wiki_path_aliases (release_id, knowledge_base_id, alias_path, alias_filename, target_page_key, reason, expires_at) "
        "SELECT $1::uuid, knowledge_base_id, alias_path, alias_filename, target_page_key, reason, expires_at FROM wiki_path_aliases WHERE release_id = $2::uuid",
        release_id,
        active_release_id,
    )
    return release_id, active_release_id


async def get_release_pages(conn: asyncpg.Connection, release_id: str) -> list[ReleasePage]:
    rows = await conn.fetch(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid ORDER BY path, filename",
        release_id,
    )
    return [
        ReleasePage(
            page_key=row["page_key"],
            path=_normalize_path(row["path"]),
            filename=row["filename"],
            title=row["title"],
            content=row["content"] or "",
            tags=list(row["tags"] or []),
            sort_order=row["sort_order"] or 0,
        )
        for row in rows
    ]


async def get_release_page_by_page_key(conn: asyncpg.Connection, release_id: str, page_key: str) -> ReleasePage | None:
    row = await conn.fetchrow(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid",
        release_id,
        page_key,
    )
    if not row:
        return None
    return ReleasePage(
        page_key=row["page_key"],
        path=_normalize_path(row["path"]),
        filename=row["filename"],
        title=row["title"],
        content=row["content"] or "",
        tags=list(row["tags"] or []),
        sort_order=row["sort_order"] or 0,
    )


async def get_release_page_by_full_path(conn: asyncpg.Connection, release_id: str, full_path: str) -> ReleasePage | None:
    path, filename = _split_full_path(full_path)
    row = await conn.fetchrow(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid AND path = $2 AND filename = $3",
        release_id,
        path,
        filename,
    )
    if not row:
        return None
    return ReleasePage(
        page_key=row["page_key"],
        path=_normalize_path(row["path"]),
        filename=row["filename"],
        title=row["title"],
        content=row["content"] or "",
        tags=list(row["tags"] or []),
        sort_order=row["sort_order"] or 0,
    )


async def upsert_release_page(
    conn: asyncpg.Connection,
    release_id: str,
    *,
    path: str,
    filename: str,
    title: str | None,
    content: str,
    tags: list[str] | None = None,
    sort_order: int = 0,
    page_key: str | None = None,
) -> ReleasePage:
    normalized_path = _normalize_path(path)
    existing = None
    if page_key:
        existing = await get_release_page_by_page_key(conn, release_id, page_key)
    else:
        existing = await get_release_page_by_full_path(conn, release_id, _full_path(normalized_path, filename))
    page_key = existing.page_key if existing else (page_key or str(uuid4()))
    await conn.execute(
        "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::text[], $8) "
        "ON CONFLICT (release_id, page_key) DO UPDATE SET path = EXCLUDED.path, filename = EXCLUDED.filename, title = EXCLUDED.title, content = EXCLUDED.content, tags = EXCLUDED.tags, sort_order = EXCLUDED.sort_order",
        release_id,
        page_key,
        normalized_path,
        filename,
        title,
        content,
        list(tags or []),
        sort_order,
    )
    return (await get_release_page_by_page_key(conn, release_id, page_key))  # type: ignore[return-value]


async def delete_release_page(conn: asyncpg.Connection, release_id: str, page_key: str) -> None:
    await conn.execute(
        "DELETE FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid",
        release_id,
        page_key,
    )


async def add_alias(
    conn: asyncpg.Connection,
    release_id: str,
    knowledge_base_id: str,
    *,
    alias_path: str,
    alias_filename: str,
    target_page_key: str,
    reason: str,
) -> None:
    await conn.execute(
        "INSERT INTO wiki_path_aliases (release_id, knowledge_base_id, alias_path, alias_filename, target_page_key, reason, expires_at) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6, $7) "
        "ON CONFLICT (release_id, alias_path, alias_filename) DO UPDATE SET target_page_key = EXCLUDED.target_page_key, reason = EXCLUDED.reason, expires_at = EXCLUDED.expires_at",
        release_id,
        knowledge_base_id,
        _normalize_path(alias_path),
        alias_filename,
        target_page_key,
        reason,
        datetime.now(UTC) + timedelta(days=RETENTION_DAYS),
    )


async def rename_release_page(
    conn: asyncpg.Connection,
    release_id: str,
    knowledge_base_id: str,
    *,
    page_key: str,
    new_path: str,
    new_filename: str,
    new_title: str | None = None,
    tags: list[str] | None = None,
) -> ReleasePage:
    pages = await get_release_pages(conn, release_id)
    page = next((item for item in pages if item.page_key == page_key), None)
    if not page:
        raise RuntimeError(f"Wiki page {page_key} not found in release {release_id}")
    old_full_path = page.full_path
    new_full_path = _full_path(new_path, new_filename)

    for current in pages:
        rewritten = (
            rebase_relative_markdown_links(current.content, old_full_path, new_full_path)
            if current.page_key == page.page_key
            else rewrite_markdown_links_to_target(current.content, current.full_path, old_full_path, new_full_path)
        )
        if rewritten != current.content:
            await upsert_release_page(
                conn,
                release_id,
                path=current.path,
                filename=current.filename,
                title=current.title,
                content=rewritten,
                tags=current.tags,
                sort_order=current.sort_order,
                page_key=current.page_key,
            )

    updated = await upsert_release_page(
        conn,
        release_id,
        path=new_path,
        filename=new_filename,
        title=new_title if new_title is not None else page.title,
        content=(await get_release_page_by_page_key(conn, release_id, page_key)).content,  # type: ignore[union-attr]
        tags=tags if tags is not None else page.tags,
        sort_order=page.sort_order,
        page_key=page_key,
    )
    await add_alias(
        conn,
        release_id,
        knowledge_base_id,
        alias_path=page.path,
        alias_filename=page.filename,
        target_page_key=page_key,
        reason="rename",
    )
    return updated


async def merge_release_pages(
    conn: asyncpg.Connection,
    release_id: str,
    knowledge_base_id: str,
    *,
    source_page_key: str,
    target_page_key: str,
) -> ReleasePage:
    pages = {page.page_key: page for page in await get_release_pages(conn, release_id)}
    source = pages.get(source_page_key)
    target = pages.get(target_page_key)
    if not source or not target:
        raise RuntimeError("Source or target page missing for merge")

    merged_content = target.content
    if _normalize_coverage_unit(source.content) and _normalize_coverage_unit(source.content) not in _normalize_coverage_unit(target.content):
        merged_content = f"{target.content.rstrip()}\n\n## Merged from {source.title or source.filename}\n\n{source.content.strip()}\n"

    await upsert_release_page(
        conn,
        release_id,
        path=target.path,
        filename=target.filename,
        title=target.title,
        content=merged_content,
        tags=sorted(set(target.tags) | set(source.tags)),
        sort_order=min(target.sort_order, source.sort_order),
        page_key=target.page_key,
    )

    old_target = source.full_path
    new_target = target.full_path
    for current in await get_release_pages(conn, release_id):
        rewritten = rewrite_markdown_links_to_target(current.content, current.full_path, old_target, new_target)
        if rewritten != current.content:
            await upsert_release_page(
                conn,
                release_id,
                path=current.path,
                filename=current.filename,
                title=current.title,
                content=rewritten,
                tags=current.tags,
                sort_order=current.sort_order,
                page_key=current.page_key,
            )
    await add_alias(
        conn,
        release_id,
        knowledge_base_id,
        alias_path=source.path,
        alias_filename=source.filename,
        target_page_key=target.page_key,
        reason="merge",
    )
    await delete_release_page(conn, release_id, source.page_key)
    return (await get_release_page_by_page_key(conn, release_id, target.page_key))  # type: ignore[return-value]


async def split_release_page(
    conn: asyncpg.Connection,
    release_id: str,
    *,
    source_page_key: str,
    children: list[dict[str, Any]],
) -> list[ReleasePage]:
    source = await get_release_page_by_page_key(conn, release_id, source_page_key)
    if not source:
        raise RuntimeError("Source page missing for split")
    created: list[ReleasePage] = []
    link_lines: list[str] = []
    for child in children:
        child_page = await upsert_release_page(
            conn,
            release_id,
            path=child["path"],
            filename=child["filename"],
            title=child.get("title"),
            content=child.get("content") or "",
            tags=list(child.get("tags") or source.tags),
            sort_order=int(child.get("sort_order") or source.sort_order),
            page_key=str(child.get("page_key") or uuid4()),
        )
        created.append(child_page)
        link_lines.append(f"- [{child_page.title or child_page.filename}]({child_page.filename if child_page.path == source.path else posixpath.relpath(child_page.full_path, start=source.path)})")
    overview = f"# {source.title or source.filename}\n\nThis page was split into the following pages:\n\n" + "\n".join(link_lines)
    await upsert_release_page(
        conn,
        release_id,
        path=source.path,
        filename=source.filename,
        title=source.title,
        content=overview,
        tags=source.tags,
        sort_order=source.sort_order,
        page_key=source.page_key,
    )
    return created


async def apply_non_wiki_link_rewrites(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    *,
    old_target: str,
    new_target: str,
) -> None:
    docs = await conn.fetch(
        "SELECT id::text AS id, user_id::text AS user_id, knowledge_base_id::text AS knowledge_base_id, filename, path, file_type, content FROM documents WHERE knowledge_base_id = $1::uuid AND archived = false AND path NOT LIKE '/wiki/%%'",
        knowledge_base_id,
    )
    for doc in docs:
        if doc["file_type"] not in MARKDOWNISH_FILE_TYPES or not doc["content"]:
            continue
        rewritten = rewrite_markdown_links_to_target(doc["content"], _full_path(doc["path"], doc["filename"]), old_target, new_target)
        if rewritten == doc["content"]:
            continue
        await conn.execute(
            "UPDATE documents SET content = $1, version = version + 1, updated_at = now() WHERE id = $2::uuid",
            rewritten,
            doc["id"],
        )
        await store_chunks(conn, doc["id"], doc["user_id"], doc["knowledge_base_id"], chunk_text(rewritten))


async def resolve_alias(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    *,
    full_path: str,
) -> str | None:
    active_release_id = await ensure_initial_wiki_release(conn, knowledge_base_id)
    path, filename = _split_full_path(full_path)
    row = await conn.fetchrow(
        "SELECT p.path, p.filename FROM wiki_path_aliases a JOIN wiki_release_pages p ON p.release_id = a.release_id AND p.page_key = a.target_page_key WHERE a.release_id = $1::uuid AND a.knowledge_base_id = $2::uuid AND a.alias_path = $3 AND a.alias_filename = $4 AND (a.expires_at IS NULL OR a.expires_at > now())",
        active_release_id,
        knowledge_base_id,
        path,
        filename,
    )
    if not row:
        return None
    return _full_path(row["path"], row["filename"])


async def record_dirty_scope(conn: asyncpg.Connection, knowledge_base_id: str, *, full_path: str, reason: str) -> None:
    path, filename = _split_full_path(full_path)
    await conn.execute(
        "INSERT INTO wiki_dirty_scope (knowledge_base_id, path, filename, reason) VALUES ($1::uuid, $2, $3, $4)",
        knowledge_base_id,
        path,
        filename,
        reason,
    )


async def clear_dirty_scope(conn: asyncpg.Connection, knowledge_base_id: str, *, full_paths: list[str] | None = None) -> None:
    if not full_paths:
        await conn.execute("DELETE FROM wiki_dirty_scope WHERE knowledge_base_id = $1::uuid", knowledge_base_id)
        return
    tuples = [_split_full_path(item) for item in full_paths]
    await conn.executemany(
        "DELETE FROM wiki_dirty_scope WHERE knowledge_base_id = $1::uuid AND path = $2 AND filename = $3",
        [(knowledge_base_id, path, filename) for path, filename in tuples],
    )


def _resolve_internal_link(current_page: ReleasePage, href: str) -> str | None:
    href = href.strip()
    if not href or href.startswith("#") or href.startswith("http://") or href.startswith("https://") or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("data:"):
        return None
    target = href.split("#", 1)[0].split("?", 1)[0].strip()
    if not target:
        return current_page.full_path
    if target.startswith("/"):
        return posixpath.normpath(target)
    resolved = posixpath.normpath(posixpath.join(posixpath.dirname(current_page.full_path), target))
    return resolved if resolved.startswith("/") else f"/{resolved}"


async def validate_release(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    release_id: str,
    *,
    mode: Literal["compile", "streamlining", "manual", "mcp"] = "streamlining",
) -> ReleaseValidation:
    release_pages = await get_release_pages(conn, release_id)
    full_paths = {page.full_path for page in release_pages}
    aliases = await conn.fetch(
        "SELECT alias_path, alias_filename FROM wiki_path_aliases WHERE release_id = $1::uuid AND knowledge_base_id = $2::uuid AND (expires_at IS NULL OR expires_at > now())",
        release_id,
        knowledge_base_id,
    )
    alias_paths = {_full_path(row["alias_path"], row["alias_filename"]) for row in aliases}

    broken_links = 0
    for page in release_pages:
        for href in _INTERNAL_LINK_RE.findall(page.content or ""):
            resolved = _resolve_internal_link(page, href)
            if not resolved:
                continue
            if resolved.startswith("/wiki/") and resolved not in full_paths and resolved not in alias_paths:
                broken_links += 1

    base_release_id = await conn.fetchval("SELECT base_release_id::text FROM wiki_releases WHERE id = $1::uuid", release_id)
    coverage_preserved = True
    lost_units = 0
    duplicate_before = 0
    required = set()
    if base_release_id:
        base_pages = await get_release_pages(conn, base_release_id)
        required = {page.full_path for page in base_pages if page.full_path in {"/wiki/overview.md", "/wiki/log.md"}}
        base_units = set().union(*(_coverage_units(page.content) for page in base_pages)) if base_pages else set()
        draft_units = set().union(*(_coverage_units(page.content) for page in release_pages)) if release_pages else set()
        draft_blob = " ".join(sorted(draft_units))
        missing_units = {unit for unit in base_units if unit not in draft_units and unit not in draft_blob}
        if mode == "streamlining" and missing_units:
            coverage_preserved = False
            lost_units = len(missing_units)
        duplicate_before = len(base_pages) - len({_duplicate_signature(page) for page in base_pages})
    missing_required = sorted(required - full_paths)
    duplicate_after = len(release_pages) - len({_duplicate_signature(page) for page in release_pages})

    change_report = await summarize_release_changes(conn, knowledge_base_id, release_id)
    report = {
        "coverage_preserved": coverage_preserved,
        "lost_units": lost_units,
        "broken_links": broken_links,
        "missing_required_pages": missing_required,
        "duplicate_reduction": duplicate_before - duplicate_after,
        "structure_churn": change_report,
        "page_count": len(release_pages),
    }
    errors: list[str] = []
    if missing_required:
        errors.append(f"Missing required pages: {', '.join(missing_required)}")
    if broken_links:
        errors.append(f"Found {broken_links} broken internal wiki links")
    if mode == "streamlining" and not coverage_preserved:
        errors.append(f"Coverage decreased by {lost_units} normalized content blocks")
    return ReleaseValidation(ok=not errors, report=report, errors=errors)


async def summarize_release_changes(conn: asyncpg.Connection, knowledge_base_id: str, release_id: str) -> dict[str, int]:
    base_release_id = await conn.fetchval("SELECT base_release_id::text FROM wiki_releases WHERE id = $1::uuid", release_id)
    if not base_release_id:
        return {"created": 0, "renamed": 0, "deleted": 0, "aliases": 0, "split": 0, "merged": 0}
    base_pages = {page.page_key: page for page in await get_release_pages(conn, base_release_id)}
    draft_pages = {page.page_key: page for page in await get_release_pages(conn, release_id)}
    created = len(set(draft_pages) - set(base_pages))
    deleted = len(set(base_pages) - set(draft_pages))
    renamed = sum(1 for key in set(base_pages) & set(draft_pages) if base_pages[key].full_path != draft_pages[key].full_path)
    alias_rows = await conn.fetchval(
        "SELECT COUNT(*) FROM wiki_path_aliases WHERE release_id = $1::uuid AND knowledge_base_id = $2::uuid",
        release_id,
        knowledge_base_id,
    )
    return {
        "created": created,
        "renamed": renamed,
        "deleted": deleted,
        "aliases": int(alias_rows or 0),
        "merged": int(alias_rows or 0),
        "split": max(0, created - deleted),
    }


async def publish_release(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    release_id: str,
    *,
    actor_user_id: str,
    mode: Literal["compile", "streamlining", "manual", "mcp"] = "streamlining",
) -> dict[str, Any]:
    lock_key = f"wiki-publish:{knowledge_base_id}"
    acquired = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", lock_key)
    if not acquired:
        raise RuntimeError("Wiki publish already in progress")
    try:
        active_release_id = await ensure_initial_wiki_release(conn, knowledge_base_id)
        base_release_id = await conn.fetchval("SELECT base_release_id::text FROM wiki_releases WHERE id = $1::uuid", release_id)
        if base_release_id and active_release_id and base_release_id != active_release_id:
            raise RuntimeError("Draft release is stale relative to the current published wiki")

        validation = await validate_release(conn, knowledge_base_id, release_id, mode=mode)
        if not validation.ok:
            raise RuntimeError("; ".join(validation.errors))

        draft_pages = {page.page_key: page for page in await get_release_pages(conn, release_id)}
        existing_rows = await conn.fetch(
            "SELECT id::text AS id, knowledge_base_id::text AS knowledge_base_id, user_id::text AS user_id, filename, path, title, content, tags, sort_order, archived FROM documents WHERE knowledge_base_id = $1::uuid AND path LIKE '/wiki/%%' AND file_type = ANY($2::text[])",
            knowledge_base_id,
            list(_WIKI_MARKDOWN_TYPES),
        )
        existing = {row["id"]: dict(row) for row in existing_rows}

        for page in draft_pages.values():
            if page.page_key in existing:
                old = existing[page.page_key]
                unchanged = (
                    old["filename"] == page.filename
                    and old["path"] == page.path
                    and old["title"] == page.title
                    and (old["content"] or "") == page.content
                    and list(old["tags"] or []) == list(page.tags)
                    and int(old["sort_order"] or 0) == int(page.sort_order)
                    and not old["archived"]
                )
                if not unchanged:
                    await conn.execute(
                        "UPDATE documents SET filename = $1, path = $2, title = $3, content = $4, tags = $5::text[], sort_order = $6, archived = false, status = 'ready', updated_at = now(), version = version + 1 WHERE id = $7::uuid",
                        page.filename,
                        page.path,
                        page.title,
                        page.content,
                        list(page.tags),
                        page.sort_order,
                        page.page_key,
                    )
                    await store_chunks(conn, page.page_key, old["user_id"] or actor_user_id, knowledge_base_id, chunk_text(page.content))
            else:
                await conn.execute(
                    "INSERT INTO documents (id, knowledge_base_id, user_id, filename, path, title, file_type, status, content, tags, version, sort_order) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, 'md', 'ready', $7, $8::text[], 1, $9)",
                    page.page_key,
                    knowledge_base_id,
                    actor_user_id,
                    page.filename,
                    page.path,
                    page.title,
                    page.content,
                    list(page.tags),
                    page.sort_order,
                )
                await store_chunks(conn, page.page_key, actor_user_id, knowledge_base_id, chunk_text(page.content))

        removed_ids = [page_key for page_key in existing if page_key not in draft_pages]
        if removed_ids:
            await conn.execute(
                "UPDATE documents SET archived = true, updated_at = now(), version = version + 1 WHERE id = ANY($1::uuid[])",
                removed_ids,
            )

        await conn.execute(
            "UPDATE wiki_releases SET status = CASE WHEN id = $1::uuid THEN 'published' ELSE CASE WHEN status = 'published' THEN 'superseded' ELSE status END END, quality_report = CASE WHEN id = $1::uuid THEN $2::jsonb ELSE quality_report END, change_report = CASE WHEN id = $1::uuid THEN $3::jsonb ELSE change_report END, published_at = CASE WHEN id = $1::uuid THEN now() ELSE published_at END, updated_at = now() WHERE knowledge_base_id = $4::uuid",
            release_id,
            json.dumps(validation.report),
            json.dumps(await summarize_release_changes(conn, knowledge_base_id, release_id)),
            knowledge_base_id,
        )
        await conn.execute(
            "UPDATE knowledge_base_settings SET active_wiki_release_id = $1::uuid, updated_at = now() WHERE knowledge_base_id = $2::uuid",
            release_id,
            knowledge_base_id,
        )
        return validation.report
    finally:
        await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", lock_key)


async def prune_old_releases(conn: asyncpg.Connection, knowledge_base_id: str) -> None:
    active_release_id = await conn.fetchval("SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid", knowledge_base_id)
    previous_release_id = await conn.fetchval(
        "SELECT id::text FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND status IN ('published','superseded') AND id <> COALESCE($2::uuid, '00000000-0000-0000-0000-000000000000'::uuid) ORDER BY published_at DESC NULLS LAST LIMIT 1",
        knowledge_base_id,
        active_release_id,
    )
    retention_window = timedelta(days=RETENTION_DAYS)
    await conn.execute(
        "DELETE FROM wiki_path_aliases WHERE knowledge_base_id = $1::uuid AND expires_at IS NOT NULL AND expires_at < now()",
        knowledge_base_id,
    )
    await conn.execute(
        "DELETE FROM wiki_release_pages WHERE release_id IN (SELECT id FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND created_at < now() - $2::interval AND id <> COALESCE($3::uuid, '00000000-0000-0000-0000-000000000000'::uuid) AND id <> COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid))",
        knowledge_base_id,
        retention_window,
        active_release_id,
        previous_release_id,
    )
    await conn.execute(
        "DELETE FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND created_at < now() - $2::interval AND id <> COALESCE($3::uuid, '00000000-0000-0000-0000-000000000000'::uuid) AND id <> COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid)",
        knowledge_base_id,
        retention_window,
        active_release_id,
        previous_release_id,
    )


async def draft_release_stats(conn: asyncpg.Connection, release_id: str) -> dict[str, Any]:
    pages = await get_release_pages(conn, release_id)
    return {
        "page_count": len(pages),
        "paths": [page.full_path for page in pages],
    }
