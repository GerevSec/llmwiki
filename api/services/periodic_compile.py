from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx

from config import settings
from services.compile_logging import log_compile, preview
from services.compile_tools import (
    ToolContext,
    execute_tool,
    tool_definitions_anthropic,
    tool_definitions_openrouter,
)
from services.encryption import decrypt_secret
from services.llm_json import loads_lenient_json
from services.openrouter_client import post_openrouter_chat_completion
from services.wiki_releases import create_draft_release, get_release_pages, publish_release, record_dirty_scope

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
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
    run_id: str | None = None
    run_started_at: datetime | None = None
    force_all_sources: bool = False
    reset_wiki: bool = False
    pending_source_paths: tuple[str, ...] = ()


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
    content_chars: int
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
        "content_chars": source.content_chars,
        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
        "full_path": source.full_path,
    }


def _chunk_pending_sources(sources: list[PendingSource], *, batch_size: int) -> list[list[PendingSource]]:
    size = max(1, batch_size)
    return [sources[index:index + size] for index in range(0, len(sources), size)]


def _effective_recompile_batch_size(target: CompileTarget) -> int:
    if not target.reset_wiki:
        return target.max_sources
    return max(1, min(target.max_sources, settings.LLMWIKI_RECOMPILE_BATCH_MAX_SOURCES))


def _build_recompile_batch_instructions(batch_index: int, total_batches: int) -> str:
    return (
        f"This is batch {batch_index} of {total_batches} in a from-scratch rebuild. "
        "Continue updating the same draft wiki with only the listed sources for this batch. "
        "Preserve draft wiki pages created earlier in this rebuild, and do not restart from zero. "
        "Before you finish this batch, make concrete wiki edits for the facts you extracted: update/create the relevant pages, refresh overview, and add the batch's ingest note."
    )


def _run_timeout_seconds(target: CompileTarget) -> int:
    if target.reset_wiki:
        return settings.LLMWIKI_RECOMPILE_RUN_TIMEOUT_SECONDS
    return settings.LLMWIKI_COMPILE_RUN_TIMEOUT_SECONDS


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
        if int(row.get("content_chars") or 0) <= 0:
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
                content_chars=int(row.get("content_chars") or 0),
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
        "Structure guidance:",
        "- Keep the wiki organized around durable subject-matter categories instead of ad-hoc one-off folders.",
        "- Reuse existing category structure when it is coherent; improve it only when there is a clear structural gain.",
        "- Prefer a shallow tree: `/wiki/overview.md`, `/wiki/log.md`, and one folder per domain such as `/wiki/architecture/overview.md`, `/wiki/business/pricing.md`, `/wiki/team/members.md`.",
        "- When creating a leaf page, always pass a full file path that ends in `.md` (e.g. `/wiki/team/members.md`). Never pass `/wiki/team.md` as a directory with a separate filename — that produces nested `/wiki/team.md/members.md` paths which the UI cannot navigate.",
        "- Avoid one-entry wrapper directories. If a domain only has one page, put it directly under `/wiki/<domain>.md`.",
        "- If a source belongs in an existing section, update that section rather than creating a parallel structure.",
        "",
        "Changed sources:",
    ]
    for source in sources:
        lines.append(f"- `{source.full_path}` (version {source.version})")
    if extra_prompt:
        lines.extend(["", "Additional instructions:", extra_prompt])
    return "\n".join(lines)


async def build_compile_wiki_structure_guidance(conn: asyncpg.Connection, knowledge_base_id: str) -> str:
    active_release_id = await conn.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        knowledge_base_id,
    )
    if not active_release_id:
        return ""

    pages = await get_release_pages(conn, active_release_id)
    if not pages:
        return ""

    top_level: dict[str, list[str]] = {}
    for page in pages:
        relative = page.full_path.replace("/wiki/", "", 1)
        if not relative:
            continue
        parts = [part for part in relative.split("/") if part]
        if not parts:
            continue
        category = "root" if len(parts) == 1 else parts[0]
        top_level.setdefault(category, [])
        top_level[category].append(parts[-1])

    lines = ["Current wiki structure to preserve/reuse when sensible:"]
    for category in sorted(top_level):
        examples = ", ".join(sorted(dict.fromkeys(top_level[category]))[:4])
        lines.append(f"- {category}: {examples}")
    return "\n".join(lines)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _new_compile_telemetry() -> dict[str, Any]:
    return {
        "provider_requests": 0,
        "tool_rounds": 0,
        "tool_calls": 0,
        "tool_calls_by_name": {},
        "distinct_tool_signatures": 0,
        "continuations": 0,
        "no_progress_rounds": 0,
        "progress_events": 0,
        "provider_request_ids": [],
        "abort_reason": None,
        "last_meaningful_progress_at": None,
    }


def _tool_result_made_progress(tool_name: str, result_text: str) -> bool:
    if tool_name not in {"write", "delete"}:
        return False
    lowered = (result_text or "").strip().lower()
    return bool(lowered) and not lowered.startswith("error:")


def _tool_signature(tool_name: str, arguments: dict[str, Any] | None) -> str:
    return f"{tool_name}:{json.dumps(arguments or {}, sort_keys=True, default=str)}"


def _normalize_compile_tool_path(path: str | None) -> str:
    if not path:
        return ""
    cleaned = path.strip()
    if not cleaned:
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return re.sub(r"/+", "/", cleaned.rstrip("/"))


def _compile_tool_made_meaningful_progress(
    target: CompileTarget,
    tool_name: str,
    arguments: dict[str, Any] | None,
    result_text: str,
    seen_source_reads: set[str],
) -> bool:
    if _tool_result_made_progress(tool_name, result_text):
        return True
    if tool_name != "read":
        return False
    normalized_path = _normalize_compile_tool_path((arguments or {}).get("path"))
    if not normalized_path:
        return False
    if normalized_path in seen_source_reads:
        return False
    pending_paths = {item.rstrip("/") for item in target.pending_source_paths}
    if normalized_path in pending_paths:
        seen_source_reads.add(normalized_path)
        return True
    return False


def _mark_progress_event(telemetry: dict[str, Any], *, progress_made: bool) -> None:
    telemetry["progress_events"] += 1 if progress_made else 0
    telemetry["no_progress_rounds"] = 0 if progress_made else telemetry["no_progress_rounds"] + 1
    if progress_made:
        telemetry["last_meaningful_progress_at"] = datetime.now(UTC).isoformat()


def _openrouter_message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return ""


def _openrouter_completion_succeeded(message: dict[str, Any], finish_reason: str | None) -> bool:
    if finish_reason in OPENROUTER_SUCCESS_FINISH_REASONS:
        return True
    if finish_reason is None and _openrouter_message_text(message).strip():
        return True
    return False


def _openrouter_empty_terminal_response(data: dict[str, Any], message: dict[str, Any], finish_reason: str | None) -> bool:
    usage = data.get("usage") or {}
    total_tokens = usage.get("total_tokens")
    return (
        finish_reason is None
        and not (message.get("tool_calls") or [])
        and not _openrouter_message_text(message).strip()
        and not message.get("reasoning")
        and total_tokens == 0
    )


async def _update_run_telemetry(
    conn: asyncpg.Connection,
    run_id: str,
    telemetry: dict[str, Any],
    *,
    progress: bool = False,
) -> None:
    await conn.execute(
        "UPDATE compile_runs SET telemetry = $1::jsonb, last_progress_at = CASE WHEN $2 THEN now() ELSE last_progress_at END WHERE id = $3::uuid",
        json.dumps(_json_ready(telemetry)),
        progress,
        run_id,
    )


async def _cleanup_stale_compile_runs(conn: asyncpg.Connection, *, stale_after_seconds: int) -> int:
    rows = await conn.fetch(
        "SELECT id::text AS id FROM compile_runs "
        "WHERE status = 'running' AND COALESCE(last_progress_at, started_at) < now() - make_interval(secs => $1)",
        stale_after_seconds,
    )
    if not rows:
        return 0
    await conn.execute(
        "UPDATE compile_runs SET status = 'failed', error_message = COALESCE(error_message, $1), finished_at = COALESCE(finished_at, now()) WHERE id = ANY($2::uuid[])",
        "Compile marked stale after exceeding the allowed run window without completing.",
        [row["id"] for row in rows],
    )
    return len(rows)


async def get_compile_context(
    pool_or_conn,
    knowledge_base_slug: str,
    max_sources: int,
    *,
    ignore_checkpoints: bool = False,
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
                ", length(coalesce(content, '')) AS content_chars "
                "FROM documents WHERE knowledge_base_id = $1::uuid",
                kb["id"],
            )
        ]
        pending = filter_pending_sources(document_rows, {} if ignore_checkpoints else checkpoints, max_sources)
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


def _compile_abort_reason(target: CompileTarget, telemetry: dict[str, Any]) -> str | None:
    if target.run_started_at:
        elapsed = (datetime.now(UTC) - target.run_started_at).total_seconds()
        if elapsed > _run_timeout_seconds(target):
            telemetry["abort_reason"] = "run_timeout"
            return "Compile aborted after exceeding total run timeout without completing"
    if telemetry.get("no_progress_rounds", 0) >= settings.LLMWIKI_COMPILE_NO_PROGRESS_ROUNDS:
        grace_anchor = target.run_started_at
        last_progress_at = telemetry.get("last_meaningful_progress_at")
        if isinstance(last_progress_at, str):
            try:
                grace_anchor = datetime.fromisoformat(last_progress_at)
            except ValueError:
                grace_anchor = target.run_started_at
        if grace_anchor:
            grace_elapsed = (datetime.now(UTC) - grace_anchor).total_seconds()
            if grace_elapsed < settings.LLMWIKI_COMPILE_NO_PROGRESS_GRACE_SECONDS:
                return None
        telemetry["abort_reason"] = "no_progress"
        return "Compile aborted after repeated rounds without meaningful wiki progress"
    return None


async def run_target(pool: asyncpg.Pool, target: CompileTarget, *, advance_schedule: bool = False) -> dict[str, Any]:
    async with pool.acquire() as conn:
        await _cleanup_stale_compile_runs(conn, stale_after_seconds=settings.LLMWIKI_COMPILE_STALE_AFTER_SECONDS)
        kb, _ = await get_compile_context(conn, target.knowledge_base, target.max_sources)
        acquired = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))",
            f"compile:{kb['id']}",
        )
        if not acquired:
            log_compile(
                "run_lock_busy",
                kb=kb["slug"],
                kb_id=kb["id"],
                mode="recompile" if target.reset_wiki else "compile",
            )
            raise RuntimeError(f"Compile already running for knowledge base '{kb['slug']}'")
        try:
            _, pending = await get_compile_context(
                conn,
                target.knowledge_base,
                target.max_sources,
                ignore_checkpoints=target.force_all_sources,
            )
            run_mode = "recompile" if target.reset_wiki else "compile"
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
                log_compile(
                    "run_skipped",
                    kb=kb["slug"],
                    kb_id=kb["id"],
                    mode=run_mode,
                    provider=target.provider,
                    model=target.model,
                    reason="no_pending_sources",
                    run_id=run_id,
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
            run_wall_start = time.monotonic()
            log_compile(
                "run_start",
                kb=kb["slug"],
                kb_id=kb["id"],
                mode=run_mode,
                provider=target.provider,
                model=target.model,
                source_count=len(pending),
                max_sources=target.max_sources,
                max_tool_rounds=target.max_tool_rounds,
                max_tokens=target.max_tokens,
                run_id=run_id,
                advance_schedule=advance_schedule,
                force_all_sources=target.force_all_sources,
                reset_wiki=target.reset_wiki,
            )
            try:
                draft_release_id, _base_release_id = await create_draft_release(
                    conn,
                    kb["id"],
                    created_by="compile",
                    created_by_run_id=run_id,
                    preserve_existing_pages=not target.reset_wiki,
                )
                run_started_at = datetime.now(UTC)
                source_batches = _chunk_pending_sources(pending, batch_size=_effective_recompile_batch_size(target))
                batch_responses: list[dict[str, Any]] = []
                for batch_index, source_batch in enumerate(source_batches, start=1):
                    structure_guidance = "" if target.reset_wiki else await build_compile_wiki_structure_guidance(conn, kb["id"])
                    prompt_parts = [target.prompt.strip(), structure_guidance.strip()]
                    if target.reset_wiki and len(source_batches) > 1:
                        prompt_parts.append(_build_recompile_batch_instructions(batch_index, len(source_batches)))
                    merged_prompt = "\n\n".join(part for part in prompt_parts if part)
                    prompt = build_compile_prompt(kb["slug"], source_batch, merged_prompt)
                    batch_wall_start = time.monotonic()
                    log_compile(
                        "batch_start",
                        kb=kb["slug"],
                        mode=run_mode,
                        run_id=run_id,
                        batch=batch_index,
                        batch_count=len(source_batches),
                        batch_sources=[source.full_path for source in source_batch],
                        prompt_chars=len(prompt),
                    )
                    batch_response = await _invoke_provider(
                        prompt,
                        replace(
                            target,
                            wiki_release_id=draft_release_id,
                            knowledge_base_id=kb["id"],
                            run_id=run_id,
                            run_started_at=run_started_at,
                            pending_source_paths=tuple(source.full_path for source in source_batch),
                        ),
                    )
                    batch_responses.append(batch_response)
                    log_compile(
                        "batch_end",
                        kb=kb["slug"],
                        mode=run_mode,
                        run_id=run_id,
                        batch=batch_index,
                        batch_count=len(source_batches),
                        stop_reason=batch_response.get("stop_reason"),
                        request_id=batch_response.get("request_id"),
                        elapsed_s=round(time.monotonic() - batch_wall_start, 2),
                        excerpt=preview(batch_response.get("text_excerpt", "")),
                    )

                quality_report = await publish_release(
                    conn,
                    kb["id"],
                    draft_release_id,
                    actor_user_id=target.actor_user_id,
                    mode="compile",
                )
                await _mark_sources_compiled(conn, run_id, kb["id"], pending)
                response_excerpt = "\n\n".join(
                    item["text_excerpt"].strip()
                    for item in batch_responses
                    if item.get("text_excerpt")
                )[:4000] or None
                response = batch_responses[-1]
                await _finish_run(conn, run_id, "succeeded", response_excerpt=response_excerpt)
                await _update_kb_settings_run_state(conn, kb["id"], "succeeded", None, advance_schedule, target.interval_minutes)
                for source in pending:
                    await record_dirty_scope(conn, kb["id"], full_path=source.full_path, reason="compile")
                log_compile(
                    "run_success",
                    kb=kb["slug"],
                    mode=run_mode,
                    run_id=run_id,
                    source_count=len(pending),
                    batch_count=len(source_batches),
                    stop_reason=response.get("stop_reason"),
                    request_id=response.get("request_id"),
                    elapsed_s=round(time.monotonic() - run_wall_start, 2),
                )
                return {
                    "knowledge_base": kb["slug"],
                    "status": "succeeded",
                    "source_count": len(pending),
                    "stop_reason": response["stop_reason"],
                    "request_id": response["request_id"],
                    "quality_report": quality_report,
                }
            except Exception as exc:
                await _finish_run(conn, run_id, "failed", error_message=str(exc))
                await _update_kb_settings_run_state(conn, kb["id"], "failed", str(exc), advance_schedule, target.interval_minutes)
                log_compile(
                    "run_failed",
                    kb=kb["slug"],
                    mode=run_mode,
                    run_id=run_id,
                    elapsed_s=round(time.monotonic() - run_wall_start, 2),
                    error=preview(str(exc), 320),
                )
                raise
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", f"compile:{kb['id']}")


async def run_due_schedules(pool: asyncpg.Pool, *, concurrency: int = 3) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        await _cleanup_stale_compile_runs(conn, stale_after_seconds=settings.LLMWIKI_COMPILE_STALE_AFTER_SECONDS)
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
    telemetry = _new_compile_telemetry()
    seen_tool_signatures: set[str] = set()
    seen_source_reads: set[str] = set()
    async with httpx.AsyncClient(timeout=timeout) as client:
        for round_index in range(target.max_tool_rounds):
            abort_reason = _compile_abort_reason(target, telemetry)
            if abort_reason:
                log_compile(
                    "provider_abort",
                    provider="anthropic",
                    kb=target.knowledge_base,
                    run_id=target.run_id,
                    round=round_index,
                    reason=telemetry.get("abort_reason") or "abort",
                    message=preview(abort_reason),
                )
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                raise RuntimeError(abort_reason)
            request_start = time.monotonic()
            log_compile(
                "provider_request",
                provider="anthropic",
                kb=target.knowledge_base,
                run_id=target.run_id,
                round=round_index,
                model=target.model,
                message_count=len(messages),
            )
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
            telemetry["provider_requests"] += 1
            if data.get("id"):
                telemetry["provider_request_ids"].append(data["id"])
            stop_reason = data.get("stop_reason")
            content_blocks = data.get("content", [])
            usage = data.get("usage") or {}
            log_compile(
                "provider_response",
                provider="anthropic",
                kb=target.knowledge_base,
                run_id=target.run_id,
                round=round_index,
                request_id=data.get("id"),
                stop_reason=stop_reason,
                elapsed_s=round(time.monotonic() - request_start, 2),
                input_tokens=usage.get("input_tokens"),
                output_tokens=usage.get("output_tokens"),
                cache_read=usage.get("cache_read_input_tokens"),
                cache_create=usage.get("cache_creation_input_tokens"),
            )
            if stop_reason == "pause_turn":
                telemetry["continuations"] += 1
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                messages.append({"role": "assistant", "content": content_blocks})
                continue
            if stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": content_blocks})
                tool_results = []
                telemetry["tool_rounds"] += 1
                progress_made = False
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as tool_conn:
                    for block in content_blocks:
                        if block.get("type") != "tool_use":
                            continue
                        tool_name = block["name"]
                        tool_input = block.get("input")
                        telemetry["tool_calls"] += 1
                        telemetry["tool_calls_by_name"][tool_name] = telemetry["tool_calls_by_name"].get(tool_name, 0) + 1
                        signature = _tool_signature(tool_name, tool_input)
                        if signature not in seen_tool_signatures:
                            seen_tool_signatures.add(signature)
                            telemetry["distinct_tool_signatures"] = len(seen_tool_signatures)
                        tool_start = time.monotonic()
                        result_text = await execute_tool(
                            ToolContext(
                                pool=tool_conn,
                                user_id=target.actor_user_id,
                                knowledge_base_slug=target.knowledge_base,
                                wiki_release_id=target.wiki_release_id,
                            ),
                            tool_name,
                            tool_input,
                        )
                        made_progress = _compile_tool_made_meaningful_progress(target, tool_name, tool_input, result_text, seen_source_reads)
                        if made_progress:
                            progress_made = True
                        log_compile(
                            "tool_call",
                            provider="anthropic",
                            kb=target.knowledge_base,
                            run_id=target.run_id,
                            round=round_index,
                            tool=tool_name,
                            args=preview(tool_input, 160),
                            result=preview(result_text, 200),
                            progress=made_progress,
                            elapsed_s=round(time.monotonic() - tool_start, 2),
                        )
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block["id"],
                                "content": result_text,
                            }
                        )
                _mark_progress_event(telemetry, progress_made=progress_made)
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry, progress=progress_made)
                messages.append({"role": "user", "content": tool_results})
                continue
            if stop_reason not in ANTHROPIC_SUCCESS_STOP_REASONS:
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                raise RuntimeError(f"Anthropic compile did not complete successfully (stop_reason={stop_reason})")
            text_excerpt = "\n".join(block.get("text", "") for block in content_blocks if block.get("type") == "text")[:2000]
            if target.run_id:
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as telemetry_conn:
                    await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
            return {"stop_reason": stop_reason or "unknown", "request_id": data.get("id", ""), "text_excerpt": text_excerpt}
    raise RuntimeError("Anthropic compile exceeded tool round limit")


async def _invoke_openrouter(prompt: str, target: CompileTarget) -> dict[str, Any]:
    tools = tool_definitions_openrouter()
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    timeout = httpx.Timeout(settings.LLMWIKI_COMPILE_TIMEOUT_SECONDS)
    telemetry = _new_compile_telemetry()
    seen_tool_signatures: set[str] = set()
    seen_source_reads: set[str] = set()
    async with httpx.AsyncClient(timeout=timeout) as client:
        for round_index in range(target.max_tool_rounds):
            abort_reason = _compile_abort_reason(target, telemetry)
            if abort_reason:
                log_compile(
                    "provider_abort",
                    provider="openrouter",
                    kb=target.knowledge_base,
                    run_id=target.run_id,
                    round=round_index,
                    reason=telemetry.get("abort_reason") or "abort",
                    message=preview(abort_reason),
                )
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                raise RuntimeError(abort_reason)
            request_start = time.monotonic()
            log_compile(
                "provider_request",
                provider="openrouter",
                kb=target.knowledge_base,
                run_id=target.run_id,
                round=round_index,
                model=target.model,
                message_count=len(messages),
            )
            data = await post_openrouter_chat_completion(
                client,
                api_key=target.provider_api_key,
                title="LLM Wiki",
                payload={
                    "model": target.model,
                    "messages": messages,
                    "tools": tools,
                    "tool_choice": "auto",
                    "max_tokens": target.max_tokens,
                },
            )
            telemetry["provider_requests"] += 1
            if data.get("id"):
                telemetry["provider_request_ids"].append(data["id"])
            choice = data["choices"][0]
            message = choice.get("message", {})
            tool_calls = message.get("tool_calls") or []
            finish_reason = choice.get("finish_reason")
            usage = data.get("usage") or {}
            log_compile(
                "provider_response",
                provider="openrouter",
                kb=target.knowledge_base,
                run_id=target.run_id,
                round=round_index,
                request_id=data.get("id"),
                finish_reason=finish_reason,
                tool_calls=len(tool_calls),
                elapsed_s=round(time.monotonic() - request_start, 2),
                prompt_tokens=usage.get("prompt_tokens"),
                completion_tokens=usage.get("completion_tokens"),
                total_tokens=usage.get("total_tokens"),
                content_preview=preview(_openrouter_message_text(message), 200),
            )
            if tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": message.get("content") or "",
                        "tool_calls": tool_calls,
                    }
                )
                telemetry["tool_rounds"] += 1
                progress_made = False
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as tool_conn:
                    for tool_call in tool_calls:
                        tool_name = tool_call["function"]["name"]
                        tool_input = loads_lenient_json(tool_call["function"]["arguments"] or "{}")
                        telemetry["tool_calls"] += 1
                        telemetry["tool_calls_by_name"][tool_name] = telemetry["tool_calls_by_name"].get(tool_name, 0) + 1
                        signature = _tool_signature(tool_name, tool_input)
                        if signature not in seen_tool_signatures:
                            seen_tool_signatures.add(signature)
                            telemetry["distinct_tool_signatures"] = len(seen_tool_signatures)
                        tool_start = time.monotonic()
                        result_text = await execute_tool(
                            ToolContext(
                                pool=tool_conn,
                                user_id=target.actor_user_id,
                                knowledge_base_slug=target.knowledge_base,
                                wiki_release_id=target.wiki_release_id,
                            ),
                            tool_name,
                            tool_input,
                        )
                        made_progress = _compile_tool_made_meaningful_progress(target, tool_name, tool_input, result_text, seen_source_reads)
                        if made_progress:
                            progress_made = True
                        log_compile(
                            "tool_call",
                            provider="openrouter",
                            kb=target.knowledge_base,
                            run_id=target.run_id,
                            round=round_index,
                            tool=tool_name,
                            args=preview(tool_input, 160),
                            result=preview(result_text, 200),
                            progress=made_progress,
                            elapsed_s=round(time.monotonic() - tool_start, 2),
                        )
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call["id"],
                                "name": tool_name,
                                "content": result_text,
                            }
                        )
                _mark_progress_event(telemetry, progress_made=progress_made)
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry, progress=progress_made)
                continue
            if _openrouter_empty_terminal_response(data, message, finish_reason):
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                continue
            if not _openrouter_completion_succeeded(message, finish_reason):
                if target.run_id:
                    tool_pool = await _get_pool_for_tools()
                    async with tool_pool.acquire() as telemetry_conn:
                        await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
                raise RuntimeError(f"OpenRouter compile did not complete successfully (finish_reason={finish_reason})")
            if target.run_id:
                tool_pool = await _get_pool_for_tools()
                async with tool_pool.acquire() as telemetry_conn:
                    await _update_run_telemetry(telemetry_conn, target.run_id, telemetry)
            return {
                "stop_reason": finish_reason or "unknown",
                "request_id": data.get("id", ""),
                "text_excerpt": _openrouter_message_text(message)[:2000],
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
        "(knowledge_base_id, user_id, status, model, provider, source_count, source_snapshot, response_excerpt, telemetry, last_progress_at, started_at, finished_at) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, CASE WHEN $3 = 'running' THEN now() ELSE NULL END, now(), CASE WHEN $3 = 'running' THEN NULL ELSE now() END) "
        "RETURNING id::text AS id",
        knowledge_base_id,
        user_id,
        status,
        model,
        provider,
        len(sources),
        json.dumps(_json_ready(sources)),
        response_excerpt,
        json.dumps(_new_compile_telemetry()),
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
