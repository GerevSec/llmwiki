from __future__ import annotations

import re
from datetime import timedelta
from uuid import uuid4

from db import get_pool

RETENTION_DAYS = 7


def _normalize_path(path: str | None) -> str:
    normalized = (path or "/").strip()
    if not normalized:
        return "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if not normalized.endswith("/"):
        normalized += "/"
    return re.sub(r"/+", "/", normalized)


async def create_draft_release(conn, knowledge_base_id: str, *, created_by: str) -> tuple[str, str]:
    active_release_id = await conn.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        knowledge_base_id,
    )
    if not active_release_id:
        active_release_id = str(uuid4())
        await conn.execute(
            "INSERT INTO wiki_releases (id, knowledge_base_id, status, created_by, published_at) VALUES ($1::uuid, $2::uuid, 'published', 'backfill', now())",
            active_release_id,
            knowledge_base_id,
        )
        await conn.execute(
            "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) "
            "SELECT $1::uuid, d.id, d.path, d.filename, d.title, COALESCE(d.content, ''), COALESCE(d.tags, '{}'::text[]), COALESCE(d.sort_order, 0) "
            "FROM documents d WHERE d.knowledge_base_id = $2::uuid AND NOT d.archived AND d.path LIKE '/wiki/%%' AND d.file_type IN ('md', 'txt', 'note')",
            active_release_id,
            knowledge_base_id,
        )
        await conn.execute(
            "UPDATE knowledge_base_settings SET active_wiki_release_id = $1::uuid, updated_at = now() WHERE knowledge_base_id = $2::uuid",
            active_release_id,
            knowledge_base_id,
        )
    draft_release_id = str(uuid4())
    await conn.execute(
        "INSERT INTO wiki_releases (id, knowledge_base_id, status, base_release_id, created_by) VALUES ($1::uuid, $2::uuid, 'draft', $3::uuid, $4)",
        draft_release_id,
        knowledge_base_id,
        active_release_id,
        created_by,
    )
    await conn.execute(
        "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) "
        "SELECT $1::uuid, page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $2::uuid",
        draft_release_id,
        active_release_id,
    )
    await conn.execute(
        "INSERT INTO wiki_path_aliases (release_id, knowledge_base_id, alias_path, alias_filename, target_page_key, reason, expires_at) "
        "SELECT $1::uuid, knowledge_base_id, alias_path, alias_filename, target_page_key, reason, expires_at FROM wiki_path_aliases WHERE release_id = $2::uuid",
        draft_release_id,
        active_release_id,
    )
    return draft_release_id, active_release_id


async def get_release_page_by_full_path(conn, release_id: str, full_path: str):
    clean = full_path.lstrip("/")
    if "/" in clean:
        path = _normalize_path(clean.rsplit("/", 1)[0])
        filename = clean.rsplit("/", 1)[1]
    else:
        path = "/"
        filename = clean
    row = await conn.fetchrow(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid AND path = $2 AND filename = $3",
        release_id,
        path,
        filename,
    )
    return dict(row) if row else None


async def get_release_pages(conn, release_id: str):
    rows = await conn.fetch(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid ORDER BY path, filename",
        release_id,
    )
    return [dict(row) for row in rows]


async def upsert_release_page(conn, release_id: str, *, path: str, filename: str, title: str | None, content: str, tags: list[str] | None = None, sort_order: int = 0, page_key: str | None = None):
    existing = None
    if page_key:
        existing = await conn.fetchrow(
            "SELECT page_key::text AS page_key FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid",
            release_id,
            page_key,
        )
    else:
        existing = await get_release_page_by_full_path(conn, release_id, f"{_normalize_path(path)}{filename}")
    page_key = (existing["page_key"] if existing else page_key) or str(uuid4())
    await conn.execute(
        "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, title, content, tags, sort_order) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::text[], $8) "
        "ON CONFLICT (release_id, page_key) DO UPDATE SET path = EXCLUDED.path, filename = EXCLUDED.filename, title = EXCLUDED.title, content = EXCLUDED.content, tags = EXCLUDED.tags, sort_order = EXCLUDED.sort_order",
        release_id,
        page_key,
        _normalize_path(path),
        filename,
        title,
        content,
        list(tags or []),
        sort_order,
    )
    return await get_release_page_by_full_path(conn, release_id, f"{_normalize_path(path)}{filename}")


async def delete_release_page(conn, release_id: str, page_key: str) -> None:
    await conn.execute("DELETE FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid", release_id, page_key)


async def record_dirty_scope(conn, knowledge_base_id: str, *, full_path: str, reason: str) -> None:
    clean = full_path.lstrip("/")
    path = _normalize_path(clean.rsplit("/", 1)[0]) if "/" in clean else "/"
    filename = clean.rsplit("/", 1)[1] if "/" in clean else clean
    await conn.execute(
        "INSERT INTO wiki_dirty_scope (knowledge_base_id, path, filename, reason) VALUES ($1::uuid, $2, $3, $4)",
        knowledge_base_id,
        path,
        filename,
        reason,
    )


async def publish_release(conn, knowledge_base_id: str, release_id: str, *, actor_user_id: str) -> None:
    draft_pages = await conn.fetch(
        "SELECT page_key::text AS page_key, path, filename, title, content, tags, sort_order FROM wiki_release_pages WHERE release_id = $1::uuid",
        release_id,
    )
    existing_rows = await conn.fetch(
        "SELECT id::text AS id, filename, path, title, content, tags, sort_order, archived FROM documents WHERE knowledge_base_id = $1::uuid AND path LIKE '/wiki/%%' AND file_type IN ('md', 'txt', 'note')",
        knowledge_base_id,
    )
    existing = {row['id']: dict(row) for row in existing_rows}
    draft_ids = set()
    for row in draft_pages:
        draft_ids.add(row['page_key'])
        old = existing.get(row['page_key'])
        if old:
            unchanged = (
                old['filename'] == row['filename'] and old['path'] == row['path'] and old['title'] == row['title'] and
                (old['content'] or '') == (row['content'] or '') and list(old['tags'] or []) == list(row['tags'] or []) and
                int(old['sort_order'] or 0) == int(row['sort_order'] or 0) and not old['archived']
            )
            if not unchanged:
                await conn.execute(
                    "UPDATE documents SET filename = $1, path = $2, title = $3, content = $4, tags = $5::text[], sort_order = $6, archived = false, status = 'ready', updated_at = now(), version = version + 1 WHERE id = $7::uuid",
                    row['filename'], row['path'], row['title'], row['content'], list(row['tags'] or []), row['sort_order'] or 0, row['page_key'],
                )
        else:
            await conn.execute(
                "INSERT INTO documents (id, knowledge_base_id, user_id, filename, path, title, file_type, status, content, tags, version, sort_order) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, 'md', 'ready', $7, $8::text[], 1, $9)",
                row['page_key'], knowledge_base_id, actor_user_id, row['filename'], row['path'], row['title'], row['content'], list(row['tags'] or []), row['sort_order'] or 0,
            )
    removed = [doc_id for doc_id in existing if doc_id not in draft_ids]
    if removed:
        await conn.execute("UPDATE documents SET archived = true, updated_at = now(), version = version + 1 WHERE id = ANY($1::uuid[])", removed)
    await conn.execute(
        "UPDATE wiki_releases SET status = CASE WHEN id = $1::uuid THEN 'published' ELSE CASE WHEN status = 'published' THEN 'superseded' ELSE status END END, published_at = CASE WHEN id = $1::uuid THEN now() ELSE published_at END, updated_at = now() WHERE knowledge_base_id = $2::uuid",
        release_id,
        knowledge_base_id,
    )
    await conn.execute(
        "UPDATE knowledge_base_settings SET active_wiki_release_id = $1::uuid, updated_at = now() WHERE knowledge_base_id = $2::uuid",
        release_id,
        knowledge_base_id,
    )


async def prune_old_releases(conn, knowledge_base_id: str) -> None:
    active_release_id = await conn.fetchval("SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid", knowledge_base_id)
    previous_release_id = await conn.fetchval(
        "SELECT id::text FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND status IN ('published','superseded') AND id <> COALESCE($2::uuid, '00000000-0000-0000-0000-000000000000'::uuid) ORDER BY published_at DESC NULLS LAST LIMIT 1",
        knowledge_base_id,
        active_release_id,
    )
    retention_window = timedelta(days=RETENTION_DAYS)
    await conn.execute("DELETE FROM wiki_path_aliases WHERE knowledge_base_id = $1::uuid AND expires_at IS NOT NULL AND expires_at < now()", knowledge_base_id)
    await conn.execute(
        "DELETE FROM wiki_release_pages WHERE release_id IN (SELECT id FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND created_at < now() - $2::interval AND id <> COALESCE($3::uuid, '00000000-0000-0000-0000-000000000000'::uuid) AND id <> COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid))",
        knowledge_base_id, retention_window, active_release_id, previous_release_id,
    )
    await conn.execute(
        "DELETE FROM wiki_releases WHERE knowledge_base_id = $1::uuid AND created_at < now() - $2::interval AND id <> COALESCE($3::uuid, '00000000-0000-0000-0000-000000000000'::uuid) AND id <> COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid)",
        knowledge_base_id, retention_window, active_release_id, previous_release_id,
    )
