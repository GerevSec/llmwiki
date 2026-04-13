from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx

from config import settings
from services.compile_tools import (
    ToolContext,
    execute_tool,
    tool_definitions_anthropic,
    tool_definitions_openrouter,
)
from services.encryption import decrypt_secret
from services.wiki_releases import create_draft_release, publish_release, record_dirty_scope

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
ANTHROPIC_SUCCESS_STOP_REASONS = {"end_turn", "stop_sequence"}
OPENROUTER_SUCCESS_FINISH_REASONS = {"stop"}
SUPPORTED_PROVIDERS = {"anthropic", "openrouter"}
DEFAULT_MAX_SOURCES = settings.LLMWIKI_COMPILE_MAX_SOURCES
DEFAULT_MAX_TOOL_ROUNDS = settings.LLMWIKI_COMPILE_MAX_TOOL_ROUNDS
DEFAULT_MAX_TOKENS = settings.LLMWIKI_COMPILE_DEFAULT_MAX_TOKENS
DEFAULT_COMPILE_INSTRUCTIONS = """\
Keep the wiki incremental, grounded, and easy to query later.
- Read only the listed changed sources first and avoid reprocessing unchanged sources.
- Merge new facts into existing `/wiki/` pages when possible instead of creating redundant pages.
- Preserve and repair wiki links if pages move or are renamed.
- Keep overview/log current and mention what changed, what remains uncertain, and what sources were used.
- If the changed batch is too large to finish cleanly, prioritize the highest-signal sources first and leave the rest for the next run.
"""


@dataclass(frozen=True)
class CompileTarget:
    knowledge_base: str
    provider_api_key: str
    prompt: str
    max_sources: int
    provider: str
    model: str
    max_tool_rounds: int
    max_tokens: int
    actor_user_id: str
    interval_minutes: int | None = None
    wiki_release_id: str | None = None
    knowledge_base_id: str | None = None


@dataclass(frozen=True)
class CompileScheduleConfig:
    knowledge_base: str
    enabled: bool
    provider: str
    model: str | None
    interval_minutes: int
    max_sources: int
    prompt: str
    max_tool_rounds: int
    max_tokens: int


@dataclass(frozen=True)
class PendingSource:
    id: str
    path: str
    filename: str
    title: str
    version: int
    updated_at: datetime | None

    @property
    def full_path(self) -> str:
        return f"{self.path}{self.filename}".replace("//", "/")


def default_compile_provider() -> str:
    return "anthropic"


def default_model_for_provider(provider: str) -> str:
    provider = provider.strip().lower()
    if provider == "anthropic":
        return settings.ANTHROPIC_MODEL
    if provider == "openrouter":
        return settings.OPENROUTER_MODEL
    raise ValueError(f"Unsupported provider: {provider}")


def next_run_at(interval_minutes: int) -> datetime:
    return datetime.now(UTC) + timedelta(minutes=interval_minutes)


def default_max_sources() -> int:
    return DEFAULT_MAX_SOURCES


def default_max_tool_rounds() -> int:
    return DEFAULT_MAX_TOOL_ROUNDS


def default_max_tokens() -> int:
    return DEFAULT_MAX_TOKENS


def _serialize_pending_source(source: PendingSource) -> dict[str, Any]:
    return {
        "id": source.id,
        "path": source.path,
        "filename": source.filename,
        "title": source.title,
        "version": source.version,
        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
        "full_path": source.full_path,
    }


def filter_pending_sources(
    document_rows: list[dict[str, Any]],
    checkpoint_versions: dict[str, int],
    max_sources: int,
) -> list[PendingSource]:
    pending: list[PendingSource] = []
    for row in document_rows:
        path = row["path"]
        if row.get("archived"):
            continue
        if path.startswith("/wiki/"):
            continue
        if row.get("status") != "ready":
            continue
        if checkpoint_versions.get(row["id"]) == row["version"]:
            continue
        pending.append(
            PendingSource(
                id=row["id"],
                path=path,
                filename=row["filename"],
                title=row.get("title") or row["filename"],
                version=row["version"],
                updated_at=row.get("updated_at"),
            )
        )
    pending.sort(
        key=lambda row: (
            row.updated_at.timestamp() if row.updated_at else float("-inf"),
            row.id,
        )
    )
    return pending[:max_sources]


def build_compile_prompt(knowledge_base: str, sources: list[PendingSource], extra_prompt: str = "") -> str:
    lines = [
        f"Use the LLM Wiki toolset to update the knowledge base `{knowledge_base}`.",
        "Only process the listed source documents because they are new or changed since the last successful automated compile.",
        "",
        "Required workflow:",
        "1. Start by calling `guide`.",
        "2. Read each listed source document.",
        "3. Update or create wiki pages under `/wiki/` as needed.",
        "4. Update `/wiki/overview.md`.",
        "5. Append one ingest entry to `/wiki/log.md` noting this was an automated periodic compile.",
        "6. Do not reprocess unrelated sources unless required for consistency.",
        "7. Finish with a section titled `AUTOMATION SUMMARY` containing 3-6 bullets.",
        "8. If you run out of budget, prefer partially-updated wiki pages plus a clear log entry over silently skipping changed sources.",
        "",
        "Internal guidance:",
        DEFAULT_COMPILE_INSTRUCTIONS.strip(),
        "",
        "Changed sources:",
    ]
    for source in sources:
        lines.append(f"- `{source.full_path}` (version {source.version})")
    if extra_prompt:
        lines.extend(["", "Additional instructions:", extra_prompt])
    return "\n".join(lines)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


async def get_compile_context(
    pool_or_conn,
    knowledge_base_slug: str,
    max_sources: int,
) -> tuple[dict[str, Any], list[PendingSource]]:
    conn = pool_or_conn
    release = None
    if isinstance(pool_or_conn, asyncpg.Pool):
        conn = await pool_or_conn.acquire()
        release = pool_or_conn.release
    try:
        kb = await conn.fetchrow(
            "SELECT id::text AS id, user_id::text AS owner_user_id, slug, name "
            "FROM knowledge_bases WHERE slug = $1",
            knowledge_base_slug,
        )
        if not kb:
            raise RuntimeError(f"Knowledge base '{knowledge_base_slug}' not found")
        checkpoint_rows = await conn.fetch(
            "SELECT document_id::text AS document_id, compiled_version "
            "FROM compiled_source_checkpoints WHERE knowledge_base_id = $1::uuid",
            kb["id"],
        )
        checkpoints = {row["document_id"]: row["compiled_version"] for row in checkpoint_rows}
        document_rows = [
            dict(row)
            for row in await conn.fetch(
                "SELECT id::text AS id, path, filename, COALESCE(title, filename) AS title, status, archived, version, updated_at "
                "FROM documents WHERE knowledge_base_id = $1::uuid",
                kb["id"],
            )
        ]
        pending = filter_pending_sources(document_rows, checkpoints, max_sources)
        return dict(kb), pending
    finally:
        if release:
            await release(conn)


async def load_target_from_settings(pool_or_conn, knowledge_base_slug: str) -> CompileTarget:
    conn = pool_or_conn
    release = None
    if isinstance(pool_or_conn, asyncpg.Pool):
        conn = await pool_or_conn.acquire()
        release = pool_or_conn.release
    try:
        kb = await conn.fetchrow(
            "SELECT kb.slug, kb.user_id::text AS owner_user_id, s.compile_provider, s.compile_model, "
            "s.compile_interval_minutes, s.compile_max_sources, s.compile_prompt, "
            "s.compile_max_tool_rounds, s.compile_max_tokens, s.provider_secret_encrypted "
            "FROM knowledge_bases kb "
            "JOIN knowledge_base_settings s ON s.knowledge_base_id = kb.id "
            "WHERE kb.slug = $1",
            knowledge_base_slug,
        )
        if not kb:
            raise RuntimeError(f"Knowledge base '{knowledge_base_slug}' settings not found")
        if not kb["provider_secret_encrypted"]:
            raise RuntimeError(f"Knowledge base '{knowledge_base_slug}' compile provider secret is not configured")
        provider = kb["compile_provider"]
        model = kb["compile_model"] or default_model_for_provider(provider)
        if provider not in SUPPORTED_PROVIDERS:
            raise RuntimeError(f"Unsupported provider '{provider}' for knowledge base '{knowledge_base_slug}'")
        if not model:
            raise RuntimeError(f"Model is not configured for knowledge base '{knowledge_base_slug}'")
        return CompileTarget(
            knowledge_base=knowledge_base_slug,
            provider_api_key=decrypt_secret(kb["provider_secret_encrypted"]) or "",
            prompt=kb["compile_prompt"] or "",
            max_sources=kb["compile_max_sources"] or default_max_sources(),
            provider=provider,
            model=model,
            max_tool_rounds=kb["compile_max_tool_rounds"] or default_max_tool_rounds(),
            max_tokens=kb["compile_max_tokens"] or default_max_tokens(),
            actor_user_id=kb["owner_user_id"],
            interval_minutes=kb["compile_interval_minutes"],
        )
    finally:
        if release:
            await release(conn)


async def run_target(pool: asyncpg.Pool, target: CompileTarget, *, advance_schedule: bool = False) -> dict[str, Any]:
    async with pool.acquire() as conn:
        kb, _ = await get_compile_context(conn, target.knowledge_base, target.max_sources)
        acquired = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))",
            f"compile:{kb['id']}",
        )
        if not acquired:
            raise RuntimeError(f"Compile already running for knowledge base '{kb['slug']}'")
        try:
            _, pending = await get_compile_context(conn, target.knowledge_base, target.max_sources)
            if not pending:
                run_id = await _record_run(
                    conn,
                    kb["id"],
                    target.actor_user_id,
                    "skipped",
                    model=target.model,
                    provider=target.provider,
                    sources=[],
                    response_excerpt="No new or changed ready sources.",
                )
                await _update_kb_settings_run_state(conn, kb["id"], "skipped", None, advance_schedule, target.interval_minutes)
                return {"knowledge_base": kb["slug"], "status": "skipped", "source_count": 0, "request_id": run_id}

            run_id = await _record_run(
                conn,
                kb["id"],
                target.actor_user_id,
                "running",
                model=target.model,
                provider=target.provider,
                sources=[_serialize_pending_source(source) for source in pending],
            )
            draft_release_id, _base_release_id = await create_draft_release(
                conn,
                kb["id"],
                created_by="compile",
                created_by_run_id=run_id,
            )
            prompt = build_compile_prompt(kb["slug"], pending, target.prompt)
            try:
                response = await _invoke_provider(
                    prompt,
                    replace(
                        target,
                        wiki_release_id=draft_release_id,
                        knowledge_base_id=kb["id"],
                    ),
                )
            except Exception as exc:
                await _finish_run(conn, run_id, "failed", error_message=str(exc))
                await _update_kb_settings_run_state(conn, kb["id"], "failed", str(exc), advance_schedule, target.interval_minutes)
                raise

            quality_report = await publish_release(
                conn,
                kb["id"],
                draft_release_id,
                actor_user_id=target.actor_user_id,
                mode="compile",
            )
            await _mark_sources_compiled(conn, run_id, kb["id"], pending)
            await _finish_run(conn, run_id, "succeeded", response_excerpt=response["text_excerpt"])
            await _update_kb_settings_run_state(conn, kb["id"], "succeeded", None, advance_schedule, target.interval_minutes)
            for source in pending:
                await record_dirty_scope(conn, kb["id"], full_path=source.full_path, reason="compile")
            return {
                "knowledge_base": kb["slug"],
                "status": "succeeded",
                "source_count": len(pending),
                "stop_reason": response["stop_reason"],
                "request_id": response["request_id"],
                "quality_report": quality_report,
            }
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", f"compile:{kb['id']}")


async def run_due_schedules(pool: asyncpg.Pool, *, concurrency: int = 3) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        "SELECT kb.slug "
        "FROM knowledge_base_settings s "
        "JOIN knowledge_bases kb ON kb.id = s.knowledge_base_id "
        "WHERE s.auto_compile_enabled = true AND s.provider_secret_encrypted IS NOT NULL "
        "AND (s.next_run_at IS NULL OR s.next_run_at <= now()) "
        "ORDER BY s.next_run_at NULLS FIRST, kb.slug",
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def run_slug(slug: str) -> dict[str, Any]:
        async with semaphore:
            try:
                target = await load_target_from_settings(pool, slug)
                return await run_target(pool, target, advance_schedule=True)
            except Exception as exc:
                return {"knowledge_base": slug, "status": "failed", "error": str(exc)}

    return await asyncio.gather(*(run_slug(row["slug"]) for row in rows)) if rows else []


async def _invoke_provider(prompt: str, target: CompileTarget) -> dict[str, Any]:
    if target.provider == "anthropic":
        return await _invoke_anthropic(prompt, target)
    if target.provider == "openrouter":
        return await _invoke_openrouter(prompt, target)
    raise RuntimeError(f"Unsupported provider: {target.provider}")


async def _invoke_anthropic(prompt: str, target: CompileTarget) -> dict[str, Any]:
    headers = {
        "x-api-key": target.provider_api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    tools = tool_definitions_anthropic()
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    timeout = httpx.Timeout(settings.LLMWIKI_COMPILE_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for _ in range(target.max_tool_rounds):
            response = await client.post(
                ANTHROPIC_API_URL,
                headers=headers,
                json={
                    "model": target.model,
                    "max_tokens": target.max_tokens,
                    "messages": messages,
                    "tools": tools,
                },
            )
            response.raise_for_status()
            data = response.json()
            stop_reason = data.get("stop_reason")
            content_blocks = data.get("content", [])
            if stop_reason == "pause_turn":
                messages.append({"role": "assistant", "content": content_blocks})
                continue
            if stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": content_blocks})
                tool_results = []
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as tool_conn:
                    for block in content_blocks:
                        if block.get("type") != "tool_use":
                            continue
                        result_text = await execute_tool(
                            ToolContext(
                                pool=tool_conn,
                                user_id=target.actor_user_id,
                                knowledge_base_slug=target.knowledge_base,
                                wiki_release_id=target.wiki_release_id,
                            ),
                            block["name"],
                            block.get("input"),
                        )
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block["id"],
                                "content": result_text,
                            }
                        )
                messages.append({"role": "user", "content": tool_results})
                continue
            if stop_reason not in ANTHROPIC_SUCCESS_STOP_REASONS:
                raise RuntimeError(f"Anthropic compile did not complete successfully (stop_reason={stop_reason})")
            text_excerpt = "\n".join(block.get("text", "") for block in content_blocks if block.get("type") == "text")[:2000]
            return {"stop_reason": stop_reason or "unknown", "request_id": data.get("id", ""), "text_excerpt": text_excerpt}
    raise RuntimeError("Anthropic compile exceeded tool round limit")


async def _invoke_openrouter(prompt: str, target: CompileTarget) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {target.provider_api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": settings.APP_URL,
        "X-Title": "LLM Wiki",
    }
    tools = tool_definitions_openrouter()
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    timeout = httpx.Timeout(settings.LLMWIKI_COMPILE_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for _ in range(target.max_tool_rounds):
            response = await client.post(
                OPENROUTER_API_URL,
                headers=headers,
                json={
                    "model": target.model,
                    "messages": messages,
                    "tools": tools,
                    "tool_choice": "auto",
                    "max_tokens": target.max_tokens,
                },
            )
            response.raise_for_status()
            data = response.json()
            choice = data["choices"][0]
            message = choice.get("message", {})
            tool_calls = message.get("tool_calls") or []
            finish_reason = choice.get("finish_reason")
            if tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": message.get("content") or "",
                        "tool_calls": tool_calls,
                    }
                )
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as tool_conn:
                    for tool_call in tool_calls:
                        result_text = await execute_tool(
                            ToolContext(
                                pool=tool_conn,
                                user_id=target.actor_user_id,
                                knowledge_base_slug=target.knowledge_base,
                                wiki_release_id=target.wiki_release_id,
                            ),
                            tool_call["function"]["name"],
                            json.loads(tool_call["function"]["arguments"] or "{}"),
                        )
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call["id"],
                                "name": tool_call["function"]["name"],
                                "content": result_text,
                            }
                        )
                continue
            if finish_reason not in OPENROUTER_SUCCESS_FINISH_REASONS:
                raise RuntimeError(f"OpenRouter compile did not complete successfully (finish_reason={finish_reason})")
            return {
                "stop_reason": finish_reason or "unknown",
                "request_id": data.get("id", ""),
                "text_excerpt": (message.get("content") or "")[:2000],
            }
    raise RuntimeError("OpenRouter compile exceeded tool round limit")


_tool_pool: asyncpg.Pool | None = None


async def _get_pool_for_tools() -> asyncpg.Pool:
    global _tool_pool
    if _tool_pool is None:
        _tool_pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=5)
    return _tool_pool


async def _record_run(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    user_id: str,
    status: str,
    *,
    model: str,
    provider: str,
    sources: list[dict[str, Any]],
    response_excerpt: str | None = None,
) -> str:
    row = await conn.fetchrow(
        "INSERT INTO compile_runs "
        "(knowledge_base_id, user_id, status, model, provider, source_count, source_snapshot, response_excerpt, started_at, finished_at) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb, $8, now(), CASE WHEN $3 = 'running' THEN NULL ELSE now() END) "
        "RETURNING id::text AS id",
        knowledge_base_id,
        user_id,
        status,
        model,
        provider,
        len(sources),
        json.dumps(_json_ready(sources)),
        response_excerpt,
    )
    return row["id"]


async def _finish_run(
    conn: asyncpg.Connection,
    run_id: str,
    status: str,
    *,
    response_excerpt: str | None = None,
    error_message: str | None = None,
) -> None:
    await conn.execute(
        "UPDATE compile_runs SET status = $1, response_excerpt = COALESCE($2, response_excerpt), error_message = $3, finished_at = now() "
        "WHERE id = $4::uuid",
        status,
        response_excerpt,
        error_message,
        run_id,
    )


async def _mark_sources_compiled(
    conn: asyncpg.Connection,
    run_id: str,
    knowledge_base_id: str,
    sources: list[PendingSource],
) -> None:
    await conn.executemany(
        "INSERT INTO compiled_source_checkpoints (knowledge_base_id, document_id, compiled_version, compiled_at, last_run_id) "
        "VALUES ($1::uuid, $2::uuid, $3, now(), $4::uuid) "
        "ON CONFLICT (knowledge_base_id, document_id) DO UPDATE "
        "SET compiled_version = EXCLUDED.compiled_version, compiled_at = EXCLUDED.compiled_at, last_run_id = EXCLUDED.last_run_id",
        [(knowledge_base_id, source.id, source.version, run_id) for source in sources],
    )


async def _update_kb_settings_run_state(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    status: str,
    error_message: str | None,
    advance_schedule: bool,
    interval_minutes: int | None,
) -> None:
    if advance_schedule and interval_minutes:
        await conn.execute(
            "UPDATE knowledge_base_settings "
            "SET last_run_at = now(), last_status = $1, last_error = $2, next_run_at = $3, updated_at = now() "
            "WHERE knowledge_base_id = $4::uuid",
            status,
            error_message,
            next_run_at(interval_minutes),
            knowledge_base_id,
        )
    else:
        await conn.execute(
            "UPDATE knowledge_base_settings "
            "SET last_run_at = now(), last_status = $1, last_error = $2, updated_at = now() "
            "WHERE knowledge_base_id = $3::uuid",
            status,
            error_message,
            knowledge_base_id,
        )
