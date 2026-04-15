from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx

from config import settings
from services.compile_logging import log_streamline, preview
from services.encryption import decrypt_secret
from services.llm_json import loads_lenient_json
from services.openrouter_client import post_openrouter_chat_completion
from services.periodic_compile import (
    ANTHROPIC_API_URL,
    ANTHROPIC_VERSION,
    default_model_for_provider,
    next_run_at,
)
from services.wiki_releases import (
    add_alias,
    clear_dirty_scope,
    create_draft_release,
    delete_release_page,
    get_release_page_by_full_path,
    get_release_pages,
    merge_release_pages,
    prune_old_releases,
    publish_release,
    record_dirty_scope,
    rename_release_page,
    split_release_page,
    upsert_release_page,
    validate_release,
)

SUPPORTED_PROVIDERS = {"anthropic", "openrouter"}
STREAMLINING_SCOPE_TARGETED = "targeted"
STREAMLINING_SCOPE_FULL = "full"
MAX_SCOPE_PAGES = 60


@dataclass(frozen=True)
class StreamliningTarget:
    knowledge_base: str
    knowledge_base_id: str
    provider_api_key: str
    provider: str
    model: str
    prompt: str
    actor_user_id: str
    interval_minutes: int
    active_release_id: str


@dataclass(frozen=True)
class StreamliningScope:
    scope_type: str
    pages: list[dict[str, Any]]
    dirty_paths: list[str]


DEFAULT_STREAMLINING_PROMPT = """\
You are streamlining an existing wiki. Preserve all source-grounded information and nuance.
Rules:
- Do not drop facts.
- Prefer stable structure unless there is a clear coherence improvement.
- Prefer simple operations in this order: update existing canonical pages, create missing pages, merge duplicates.
- Avoid rename/move/split unless simpler update/create/merge steps cannot produce a coherent result.
- Merge duplicated pages when one canonical page can own the information.
- Renames and moves should reduce confusion, not create churn.
- If splitting a page, keep the original page as a lightweight index/landing page.
- Return ONLY valid JSON following the requested schema.
"""


def _normalize_top_dir(full_path: str) -> str:
    relative = full_path.replace("/wiki/", "", 1).strip("/")
    if not relative:
        return ""
    return relative.split("/", 1)[0]


def _normalize_page_reference(reference: str) -> str:
    ref = (reference or "").strip()
    if not ref:
        return ""
    if not ref.startswith("/"):
        ref = f"/{ref}"
    return ref.rstrip("/") if ref != "/" else ref


def _reference_slug(reference: str) -> str:
    ref = _normalize_page_reference(reference)
    tail = ref.rsplit("/", 1)[-1]
    tail = tail.removesuffix(".md").removesuffix(".txt")
    tail = re.sub(r"[^a-z0-9]+", "-", tail.lower()).strip("-")
    return tail


def _title_slug(title: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")


async def resolve_page_reference(
    conn: asyncpg.Connection,
    draft_release_id: str,
    *,
    reference: str | None = None,
    page_key: str | None = None,
) -> Any | None:
    if page_key:
        page = next((item for item in await get_release_pages(conn, draft_release_id) if item.page_key == page_key), None)
        if page:
            return page

    if not reference:
        return None

    normalized_ref = _normalize_page_reference(reference)
    page = await get_release_page_by_full_path(conn, draft_release_id, normalized_ref)
    if page:
        return page

    if "." not in normalized_ref.rsplit("/", 1)[-1]:
        for suffix in (".md", ".txt"):
            page = await get_release_page_by_full_path(conn, draft_release_id, f"{normalized_ref}{suffix}")
            if page:
                return page

    slug = _reference_slug(normalized_ref)
    candidates = []
    for item in await get_release_pages(conn, draft_release_id):
        full_path = item.full_path.rstrip("/")
        item_slug = _reference_slug(full_path)
        path_prefix = normalized_ref + "/"
        score = 0
        if full_path.startswith(path_prefix):
            score = max(score, 90)
        if item.filename in {normalized_ref.rsplit("/", 1)[-1], f"{slug}.md", f"{slug}.txt"}:
            score = max(score, 80)
        if item_slug == slug:
            score = max(score, 70)
        if _title_slug(item.title) == slug:
            score = max(score, 60)
        if score:
            candidates.append((score, len(full_path), item))

    if not candidates:
        return None
    candidates.sort(key=lambda entry: (-entry[0], entry[1]))
    best_score = candidates[0][0]
    best = [entry[2] for entry in candidates if entry[0] == best_score]
    return best[0] if len(best) == 1 else None


def _extract_json_payload(text: str) -> dict[str, Any]:
    payload = loads_lenient_json(text)
    if not isinstance(payload, dict):
        raise json.JSONDecodeError("Expected top-level JSON object", text, 0)
    return payload


async def load_streamlining_target_from_settings(pool_or_conn, knowledge_base_slug: str) -> StreamliningTarget:
    conn = pool_or_conn
    release = None
    if isinstance(pool_or_conn, asyncpg.Pool):
        conn = await pool_or_conn.acquire()
        release = pool_or_conn.release
    try:
        row = await conn.fetchrow(
            "SELECT kb.id::text AS knowledge_base_id, kb.slug, kb.user_id::text AS owner_user_id, "
            "COALESCE(s.streamlining_provider, s.compile_provider) AS provider, "
            "COALESCE(s.streamlining_model, s.compile_model) AS model, "
            "COALESCE(NULLIF(s.streamlining_prompt, ''), s.compile_prompt, '') AS prompt, "
            "COALESCE(s.streamlining_interval_minutes, 1440) AS interval_minutes, "
            "COALESCE(s.streamlining_provider_secret_encrypted, s.provider_secret_encrypted) AS provider_secret_encrypted, "
            "s.active_wiki_release_id::text AS active_wiki_release_id "
            "FROM knowledge_bases kb JOIN knowledge_base_settings s ON s.knowledge_base_id = kb.id WHERE kb.slug = $1",
            knowledge_base_slug,
        )
        if not row:
            raise RuntimeError(f"Knowledge base '{knowledge_base_slug}' streamlining settings not found")
        provider = (row["provider"] or "").strip().lower()
        if provider not in SUPPORTED_PROVIDERS:
            raise RuntimeError(f"Unsupported streamlining provider '{provider}'")
        provider_secret = decrypt_secret(row["provider_secret_encrypted"] or "") or ""
        if not provider_secret:
            raise RuntimeError(f"Knowledge base '{knowledge_base_slug}' streamlining provider secret is not configured")
        model = (row["model"] or "").strip() or default_model_for_provider(provider)
        return StreamliningTarget(
            knowledge_base=row["slug"],
            knowledge_base_id=row["knowledge_base_id"],
            provider_api_key=provider_secret,
            provider=provider,
            model=model,
            prompt=row["prompt"] or "",
            actor_user_id=row["owner_user_id"],
            interval_minutes=int(row["interval_minutes"] or 1440),
            active_release_id=row["active_wiki_release_id"],
        )
    finally:
        if release:
            await release(conn)


async def determine_streamlining_scope(conn: asyncpg.Connection, knowledge_base_id: str, active_release_id: str, *, force_full: bool = False) -> StreamliningScope:
    pages = [
        {
            "page_key": page.page_key,
            "path": page.path,
            "filename": page.filename,
            "title": page.title,
            "content": page.content,
            "tags": page.tags,
            "sort_order": page.sort_order,
            "full_path": page.full_path,
        }
        for page in await get_release_pages(conn, active_release_id)
    ]
    dirty_rows = await conn.fetch(
        "SELECT path, filename, reason FROM wiki_dirty_scope WHERE knowledge_base_id = $1::uuid ORDER BY created_at DESC LIMIT 100",
        knowledge_base_id,
    )
    dirty_paths = [f"{row['path']}{row['filename'] or ''}".replace("//", "/") for row in dirty_rows]
    if force_full or not dirty_paths:
        return StreamliningScope(STREAMLINING_SCOPE_FULL, pages[:MAX_SCOPE_PAGES], dirty_paths)

    top_dirs = {_normalize_top_dir(path) for path in dirty_paths}
    dirty_exact = set(dirty_paths)
    targeted = [page for page in pages if page["full_path"] in dirty_exact or _normalize_top_dir(page["full_path"]) in top_dirs]
    if len(dirty_exact) <= 8 and len(top_dirs) <= 2 and len(targeted) <= MAX_SCOPE_PAGES:
        return StreamliningScope(STREAMLINING_SCOPE_TARGETED, targeted, dirty_paths)
    return StreamliningScope(STREAMLINING_SCOPE_FULL, pages[:MAX_SCOPE_PAGES], dirty_paths)


def build_streamlining_prompt(target: StreamliningTarget, scope: StreamliningScope) -> str:
    page_payload = [
        {
            "page_key": page["page_key"],
            "path": page["full_path"],
            "title": page["title"],
            "tags": page["tags"],
            "content": page["content"],
        }
        for page in scope.pages
    ]
    schema = {
        "summary": "one short sentence",
        "operations": [
            {
                "type": "merge|rename|move|split|update|create|alias",
                "source_page_key": "uuid optional but preferred for existing pages",
                "target_page_key": "uuid optional but preferred for existing pages",
                "source_path": "/wiki/example.md",
                "target_path": "/wiki/example.md",
                "reason": "why this improves coherence",
            }
        ],
    }
    lines = [
        f"Streamline the wiki `{target.knowledge_base}`.",
        f"Scope type: {scope.scope_type}",
        "Return ONLY JSON matching this general schema:",
        json.dumps(schema, indent=2),
        "",
        DEFAULT_STREAMLINING_PROMPT.strip(),
    ]
    if scope.dirty_paths:
        lines.extend(["", "Dirty paths driving this run:"])
        lines.extend(f"- {item}" for item in scope.dirty_paths[:25])
    if target.prompt:
        lines.extend(["", "Additional instructions:", target.prompt.strip()])
    lines.extend(["", "Pages in scope:", json.dumps(page_payload, indent=2)])
    return "\n".join(lines)


async def _invoke_streamlining_provider(prompt: str, target: StreamliningTarget) -> dict[str, Any]:
    timeout = httpx.Timeout(settings.LLMWIKI_COMPILE_TIMEOUT_SECONDS)
    log_streamline(
        "provider_request",
        provider=target.provider,
        kb=target.knowledge_base,
        model=target.model,
        prompt_chars=len(prompt),
    )
    request_start = time.monotonic()
    if target.provider == "anthropic":
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": target.provider_api_key,
                    "anthropic-version": ANTHROPIC_VERSION,
                    "content-type": "application/json",
                },
                json={
                    "model": target.model,
                    "max_tokens": settings.LLMWIKI_COMPILE_DEFAULT_MAX_TOKENS,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            text = "\n".join(block.get("text", "") for block in data.get("content", []) if block.get("type") == "text")
            usage = data.get("usage") or {}
            log_streamline(
                "provider_response",
                provider="anthropic",
                kb=target.knowledge_base,
                request_id=data.get("id"),
                stop_reason=data.get("stop_reason"),
                elapsed_s=round(time.monotonic() - request_start, 2),
                input_tokens=usage.get("input_tokens"),
                output_tokens=usage.get("output_tokens"),
                text_preview=preview(text, 240),
            )
            return {"request_id": data.get("id", ""), "text": text}
    async with httpx.AsyncClient(timeout=timeout) as client:
        data = await post_openrouter_chat_completion(
            client,
            api_key=target.provider_api_key,
            title="LLM Wiki Streamlining",
            payload={
                "model": target.model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": settings.LLMWIKI_COMPILE_DEFAULT_MAX_TOKENS,
            },
        )
        message = data["choices"][0].get("message", {})
        text = message.get("content") or ""
        usage = data.get("usage") or {}
        log_streamline(
            "provider_response",
            provider="openrouter",
            kb=target.knowledge_base,
            request_id=data.get("id"),
            finish_reason=data["choices"][0].get("finish_reason"),
            elapsed_s=round(time.monotonic() - request_start, 2),
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            total_tokens=usage.get("total_tokens"),
            text_preview=preview(text, 240),
        )
        return {"request_id": data.get("id", ""), "text": text}


async def _record_streamlining_run(
    conn: asyncpg.Connection,
    target: StreamliningTarget,
    scope: StreamliningScope,
    *,
    status: str,
    response_excerpt: str | None = None,
    error_message: str | None = None,
    quality_report: dict[str, Any] | None = None,
) -> str:
    row = await conn.fetchrow(
        "INSERT INTO streamlining_runs (knowledge_base_id, user_id, status, provider, model, scope_type, scope_snapshot, quality_report, response_excerpt, error_message, started_at, finished_at) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, now(), CASE WHEN $3 = 'running' THEN NULL ELSE now() END) RETURNING id::text AS id",
        target.knowledge_base_id,
        target.actor_user_id,
        status,
        target.provider,
        target.model,
        scope.scope_type,
        json.dumps(scope.pages),
        json.dumps(quality_report or {}),
        response_excerpt,
        error_message,
    )
    return row["id"]


async def _finish_streamlining_run(conn: asyncpg.Connection, run_id: str, *, status: str, response_excerpt: str | None = None, error_message: str | None = None, quality_report: dict[str, Any] | None = None) -> None:
    await conn.execute(
        "UPDATE streamlining_runs SET status = $1, response_excerpt = COALESCE($2, response_excerpt), error_message = $3, quality_report = CASE WHEN $4::jsonb = '{}'::jsonb THEN quality_report ELSE $4::jsonb END, finished_at = now() WHERE id = $5::uuid",
        status,
        response_excerpt,
        error_message,
        json.dumps(quality_report or {}),
        run_id,
    )


async def _update_streamlining_schedule(conn: asyncpg.Connection, target: StreamliningTarget, *, status: str, error_message: str | None = None) -> None:
    await conn.execute(
        "UPDATE knowledge_base_settings SET last_streamlining_at = now(), last_streamlining_status = $1, last_streamlining_error = $2, next_streamlining_at = $3, updated_at = now() WHERE knowledge_base_id = $4::uuid",
        status,
        error_message,
        next_run_at(target.interval_minutes),
        target.knowledge_base_id,
    )


async def apply_streamlining_operations(conn: asyncpg.Connection, target: StreamliningTarget, draft_release_id: str, operations: list[dict[str, Any]]) -> list[str]:
    changed_paths: list[str] = []
    for operation in operations:
        op_type = (operation.get("type") or "").strip().lower()
        source_path = operation.get("source_path")
        target_path = operation.get("target_path")
        source_page_key = operation.get("source_page_key")
        target_page_key = operation.get("target_page_key")
        if op_type in {"rename", "move"}:
            if not source_path or not target_path:
                raise RuntimeError("rename/move operation requires source_path and target_path")
            page = await resolve_page_reference(conn, draft_release_id, reference=source_path, page_key=source_page_key)
            if not page:
                raise RuntimeError(f"Source page not found for operation: {source_path}")
            target_dir = target_path.rsplit("/", 1)[0] + "/" if "/" in target_path.lstrip("/") else "/wiki/"
            target_filename = target_path.rsplit("/", 1)[-1]
            updated = await rename_release_page(
                conn,
                draft_release_id,
                target.knowledge_base_id,
                page_key=page.page_key,
                new_path=target_dir,
                new_filename=target_filename,
                new_title=operation.get("title"),
                tags=operation.get("tags"),
            )
            changed_paths.append(f"{updated.path}{updated.filename}")
        elif op_type == "update":
            if not target_path:
                raise RuntimeError("update operation requires target_path")
            page = await resolve_page_reference(conn, draft_release_id, reference=target_path, page_key=target_page_key)
            existing_key = page.page_key if page else None
            target_dir = target_path.rsplit("/", 1)[0] + "/" if "/" in target_path.lstrip("/") else "/wiki/"
            target_filename = target_path.rsplit("/", 1)[-1]
            updated = await upsert_release_page(
                conn,
                draft_release_id,
                path=target_dir,
                filename=target_filename,
                title=operation.get("title") or (page.title if page else None),
                content=operation.get("content") or (page.content if page else ""),
                tags=list(operation.get("tags") or (page.tags if page else [])),
                sort_order=int(operation.get("sort_order") or (page.sort_order if page else 0)),
                page_key=existing_key,
            )
            changed_paths.append(f"{updated.path}{updated.filename}")
        elif op_type == "create":
            if not target_path:
                raise RuntimeError("create operation requires target_path")
            target_dir = target_path.rsplit("/", 1)[0] + "/" if "/" in target_path.lstrip("/") else "/wiki/"
            target_filename = target_path.rsplit("/", 1)[-1]
            created = await upsert_release_page(
                conn,
                draft_release_id,
                path=target_dir,
                filename=target_filename,
                title=operation.get("title"),
                content=operation.get("content") or "",
                tags=list(operation.get("tags") or []),
                sort_order=int(operation.get("sort_order") or 0),
            )
            changed_paths.append(f"{created.path}{created.filename}")
        elif op_type == "merge":
            if not source_path or not target_path:
                raise RuntimeError("merge operation requires source_path and target_path")
            source_page = await resolve_page_reference(conn, draft_release_id, reference=source_path, page_key=source_page_key)
            target_page = await resolve_page_reference(conn, draft_release_id, reference=target_path, page_key=target_page_key)
            if not source_page or not target_page:
                raise RuntimeError(f"merge paths must both exist (source={source_path!r}, target={target_path!r})")
            if source_page.page_key == target_page.page_key:
                changed_paths.append(f"{target_page.path}{target_page.filename}")
                continue
            merged = await merge_release_pages(
                conn,
                draft_release_id,
                target.knowledge_base_id,
                source_page_key=source_page.page_key,
                target_page_key=target_page.page_key,
            )
            changed_paths.extend([source_path, f"{merged.path}{merged.filename}"])
        elif op_type == "split":
            if not source_path or not operation.get("children"):
                raise RuntimeError("split operation requires source_path and children")
            source_page = await resolve_page_reference(conn, draft_release_id, reference=source_path, page_key=source_page_key)
            if not source_page:
                raise RuntimeError(f"split source page not found: {source_path}")
            children = await split_release_page(conn, draft_release_id, source_page_key=source_page.page_key, children=operation["children"])
            changed_paths.append(source_path)
            changed_paths.extend(f"{child.path}{child.filename}" for child in children)
        elif op_type == "alias":
            if not source_path or not target_path:
                raise RuntimeError("alias operation requires source_path and target_path")
            target_page = await resolve_page_reference(conn, draft_release_id, reference=target_path, page_key=target_page_key)
            if not target_page:
                raise RuntimeError(f"alias target page not found: {target_path}")
            alias_dir = source_path.rsplit("/", 1)[0] + "/" if "/" in source_path.lstrip("/") else "/wiki/"
            alias_filename = source_path.rsplit("/", 1)[-1]
            await add_alias(
                conn,
                draft_release_id,
                target.knowledge_base_id,
                alias_path=alias_dir,
                alias_filename=alias_filename,
                target_page_key=target_page.page_key,
                reason=operation.get("reason") or "alias",
            )
            changed_paths.append(source_path)
        elif op_type == "delete":
            ref_path = source_path or target_path
            if not ref_path:
                raise RuntimeError("delete operation requires source_path or target_path")
            page = await resolve_page_reference(
                conn,
                draft_release_id,
                reference=ref_path,
                page_key=source_page_key or target_page_key,
            )
            if not page:
                raise RuntimeError(f"delete page not found: {ref_path}")
            protected_filenames = {"overview.md", "log.md"}
            if page.path == "/wiki/" and page.filename in protected_filenames:
                raise RuntimeError(f"refusing to delete protected wiki page {page.path}{page.filename}")
            await delete_release_page(conn, draft_release_id, page.page_key)
            changed_paths.append(f"{page.path}{page.filename}")
        else:
            raise RuntimeError(f"Unsupported streamlining operation type: {op_type}")
    return changed_paths


async def run_streamlining_target(pool: asyncpg.Pool, target: StreamliningTarget, *, force_full: bool = False, manual: bool = False) -> dict[str, Any]:
    run_wall_start = time.monotonic()
    log_streamline(
        "run_start",
        kb=target.knowledge_base,
        kb_id=target.knowledge_base_id,
        provider=target.provider,
        model=target.model,
        manual=manual,
        force_full=force_full,
        active_release_id=target.active_release_id,
    )
    async with pool.acquire() as conn:
        scope = await determine_streamlining_scope(conn, target.knowledge_base_id, target.active_release_id, force_full=force_full)
        log_streamline(
            "scope_determined",
            kb=target.knowledge_base,
            scope_type=scope.scope_type,
            pages_in_scope=len(scope.pages),
            dirty_paths=len(scope.dirty_paths),
        )
        if not scope.pages:
            await _update_streamlining_schedule(conn, target, status="skipped")
            log_streamline(
                "run_skipped",
                kb=target.knowledge_base,
                reason="empty_scope",
                scope_type=scope.scope_type,
                elapsed_s=round(time.monotonic() - run_wall_start, 2),
            )
            return {"knowledge_base": target.knowledge_base, "status": "skipped", "scope_type": scope.scope_type}
        run_id = await _record_streamlining_run(conn, target, scope, status="running")
        draft_release_id, _ = await create_draft_release(conn, target.knowledge_base_id, created_by="streamlining", created_by_run_id=run_id)
    prompt = build_streamlining_prompt(target, scope)
    try:
        response = await _invoke_streamlining_provider(prompt, target)
        payload = _extract_json_payload(response["text"])
    except Exception as exc:
        async with pool.acquire() as conn:
            await _finish_streamlining_run(conn, run_id, status="failed", error_message=str(exc))
            await _update_streamlining_schedule(conn, target, status="failed", error_message=str(exc))
        log_streamline(
            "run_failed",
            kb=target.knowledge_base,
            run_id=run_id,
            stage="provider_or_parse",
            error=preview(str(exc), 320),
            elapsed_s=round(time.monotonic() - run_wall_start, 2),
        )
        raise

    operations = list(payload.get("operations") or [])
    log_streamline(
        "operations_parsed",
        kb=target.knowledge_base,
        run_id=run_id,
        operation_count=len(operations),
        summary=preview(payload.get("summary"), 160),
    )
    if not operations:
        async with pool.acquire() as conn:
            await _finish_streamlining_run(conn, run_id, status="skipped", response_excerpt=(payload.get("summary") or "No changes"))
            await _update_streamlining_schedule(conn, target, status="skipped")
        log_streamline(
            "run_skipped",
            kb=target.knowledge_base,
            run_id=run_id,
            reason="no_operations",
            elapsed_s=round(time.monotonic() - run_wall_start, 2),
        )
        return {"knowledge_base": target.knowledge_base, "status": "skipped", "scope_type": scope.scope_type, "request_id": response["request_id"]}

    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                changed_paths = await apply_streamlining_operations(conn, target, draft_release_id, operations)
                validation = await validate_release(conn, target.knowledge_base_id, draft_release_id, mode="streamlining")
                if not validation.ok:
                    raise RuntimeError("; ".join(validation.errors))
                quality_report = await publish_release(
                    conn,
                    target.knowledge_base_id,
                    draft_release_id,
                    actor_user_id=target.actor_user_id,
                    mode="streamlining",
                )
                for changed_path in changed_paths:
                    await record_dirty_scope(conn, target.knowledge_base_id, full_path=changed_path, reason="streamlining")
                await clear_dirty_scope(conn, target.knowledge_base_id, full_paths=scope.dirty_paths)
                await prune_old_releases(conn, target.knowledge_base_id)
                await _finish_streamlining_run(
                    conn,
                    run_id,
                    status="succeeded",
                    response_excerpt=(payload.get("summary") or "")[0:2000],
                    quality_report=quality_report,
                )
                await _update_streamlining_schedule(conn, target, status="succeeded")
        except Exception as exc:
            await _finish_streamlining_run(conn, run_id, status="failed", error_message=str(exc))
            await _update_streamlining_schedule(conn, target, status="failed", error_message=str(exc))
            log_streamline(
                "run_failed",
                kb=target.knowledge_base,
                run_id=run_id,
                stage="apply_or_publish",
                error=preview(str(exc), 320),
                elapsed_s=round(time.monotonic() - run_wall_start, 2),
            )
            raise
    log_streamline(
        "run_success",
        kb=target.knowledge_base,
        run_id=run_id,
        scope_type=scope.scope_type,
        operation_count=len(operations),
        changed_paths=len(changed_paths),
        elapsed_s=round(time.monotonic() - run_wall_start, 2),
    )
    return {
        "knowledge_base": target.knowledge_base,
        "status": "succeeded",
        "scope_type": scope.scope_type,
        "request_id": response["request_id"],
    }


async def run_due_streamlining(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        "SELECT kb.slug "
        "FROM knowledge_base_settings s JOIN knowledge_bases kb ON kb.id = s.knowledge_base_id "
        "WHERE s.streamlining_enabled = true "
        "AND COALESCE(s.streamlining_provider_secret_encrypted, s.provider_secret_encrypted) IS NOT NULL "
        "AND s.active_wiki_release_id IS NOT NULL "
        "AND (s.next_streamlining_at IS NULL OR s.next_streamlining_at <= now()) "
        "ORDER BY s.next_streamlining_at NULLS FIRST, kb.slug"
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        try:
            target = await load_streamlining_target_from_settings(pool, row["slug"])
            results.append(await run_streamlining_target(pool, target))
        except Exception as exc:
            results.append({"knowledge_base": row["slug"], "status": "failed", "error": str(exc)})
    return results
