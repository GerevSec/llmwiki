from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg
import httpx

from config import settings

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
ANTHROPIC_MCP_BETA = "mcp-client-2025-11-20"
SUCCESS_STOP_REASONS = {"end_turn", "stop_sequence"}


@dataclass(frozen=True)
class CompileTarget:
    knowledge_base: str
    mcp_auth_token: str
    mcp_url: str
    prompt: str
    max_sources: int


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


def load_compile_targets() -> list[CompileTarget]:
    if settings.LLMWIKI_COMPILE_TARGETS_JSON:
        raw = json.loads(settings.LLMWIKI_COMPILE_TARGETS_JSON)
        if not isinstance(raw, list):
            raise ValueError("LLMWIKI_COMPILE_TARGETS_JSON must decode to a list")
        return [_parse_target(item) for item in raw]

    if not settings.LLMWIKI_COMPILE_KB or not settings.LLMWIKI_COMPILE_MCP_TOKEN:
        raise ValueError(
            "Set LLMWIKI_COMPILE_KB + LLMWIKI_COMPILE_MCP_TOKEN or provide LLMWIKI_COMPILE_TARGETS_JSON"
        )

    return [
        CompileTarget(
            knowledge_base=settings.LLMWIKI_COMPILE_KB,
            mcp_auth_token=settings.LLMWIKI_COMPILE_MCP_TOKEN,
            mcp_url=settings.MCP_URL,
            prompt=settings.LLMWIKI_COMPILE_PROMPT,
            max_sources=settings.LLMWIKI_COMPILE_MAX_SOURCES,
        )
    ]


def _parse_target(raw: Any) -> CompileTarget:
    if not isinstance(raw, dict):
        raise ValueError("Each compile target must be an object")

    kb = str(raw.get("knowledge_base", "")).strip()
    token = str(raw.get("mcp_auth_token", "")).strip()
    mcp_url = str(raw.get("mcp_url") or settings.MCP_URL).strip()
    prompt = str(raw.get("prompt") or settings.LLMWIKI_COMPILE_PROMPT).strip()
    max_sources = int(raw.get("max_sources") or settings.LLMWIKI_COMPILE_MAX_SOURCES)

    if not kb or not token or not mcp_url:
        raise ValueError("Each target needs knowledge_base, mcp_auth_token, and mcp_url")

    return CompileTarget(
        knowledge_base=kb,
        mcp_auth_token=token,
        mcp_url=mcp_url,
        prompt=prompt,
        max_sources=max_sources,
    )


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

    pending.sort(key=lambda row: (row.updated_at or datetime.min, row.id))
    return pending[:max_sources]


def build_compile_prompt(knowledge_base: str, sources: list[PendingSource], extra_prompt: str = "") -> str:
    lines = [
        f"Use the LLM Wiki MCP tools to update the knowledge base `{knowledge_base}`.",
        "Only process the listed source documents because they are new or changed since the last successful automated compile.",
        "",
        "Required workflow:",
        "1. Call `guide` first.",
        "2. Read each listed source document.",
        "3. Update or create wiki pages under `/wiki/` as needed.",
        "4. Update `/wiki/overview.md`.",
        "5. Append one ingest entry to `/wiki/log.md` noting this was an automated periodic compile.",
        "6. Do not reprocess unrelated sources unless required for consistency.",
        "7. Finish with a section titled `AUTOMATION SUMMARY` containing 3-6 bullets.",
        "",
        "Changed sources:",
    ]

    for source in sources:
        lines.append(f"- `{source.full_path}` (version {source.version})")

    if extra_prompt:
        lines.extend(["", "Additional instructions:", extra_prompt])

    return "\n".join(lines)


async def run_target(pool: asyncpg.Pool, target: CompileTarget) -> dict[str, Any]:
    async with pool.acquire() as conn:
        kb = await conn.fetchrow(
            "SELECT id::text AS id, user_id::text AS user_id, slug, name "
            "FROM knowledge_bases WHERE slug = $1",
            target.knowledge_base,
        )
        if not kb:
            raise RuntimeError(f"Knowledge base '{target.knowledge_base}' not found")

        acquired = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))",
            f"compile:{kb['id']}",
        )
        if not acquired:
            raise RuntimeError(f"Compile already running for knowledge base '{kb['slug']}'")

        try:
            checkpoint_rows = await conn.fetch(
                "SELECT document_id::text AS document_id, compiled_version "
                "FROM compiled_source_checkpoints WHERE knowledge_base_id = $1::uuid",
                kb["id"],
            )
            checkpoints = {row["document_id"]: row["compiled_version"] for row in checkpoint_rows}

            document_rows = [
                dict(row)
                for row in await conn.fetch(
                    "SELECT id::text AS id, path, filename, COALESCE(title, filename) AS title, "
                    "status, archived, version, updated_at "
                    "FROM documents "
                    "WHERE knowledge_base_id = $1::uuid",
                    kb["id"],
                )
            ]

            pending = filter_pending_sources(document_rows, checkpoints, target.max_sources)
            if not pending:
                await _record_run(
                    conn,
                    kb["id"],
                    kb["user_id"],
                    "skipped",
                    model=settings.ANTHROPIC_MODEL,
                    sources=[],
                    response_excerpt="No new or changed ready sources.",
                )
                return {"knowledge_base": kb["slug"], "status": "skipped", "source_count": 0}

            run_id = await _record_run(
                conn,
                kb["id"],
                kb["user_id"],
                "running",
                model=settings.ANTHROPIC_MODEL,
                sources=[source.__dict__ | {"full_path": source.full_path} for source in pending],
            )

            prompt = build_compile_prompt(kb["slug"], pending, target.prompt)

            try:
                response = await _invoke_claude(prompt, target)
            except Exception as exc:
                await _finish_run(conn, run_id, "failed", error_message=str(exc))
                raise

            await _mark_sources_compiled(conn, run_id, kb["id"], pending)
            await _finish_run(
                conn,
                run_id,
                "succeeded",
                response_excerpt=response["text_excerpt"],
            )

            return {
                "knowledge_base": kb["slug"],
                "status": "succeeded",
                "source_count": len(pending),
                "stop_reason": response["stop_reason"],
                "request_id": response["request_id"],
            }
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", f"compile:{kb['id']}")


async def _invoke_claude(prompt: str, target: CompileTarget) -> dict[str, Any]:
    if settings.LLMWIKI_COMPILE_DRY_RUN:
        return {
            "stop_reason": "dry_run",
            "request_id": "dry-run",
            "text_excerpt": prompt[:500],
        }

    headers = {
        "x-api-key": settings.ANTHROPIC_API_KEY,
        "anthropic-version": ANTHROPIC_VERSION,
        "anthropic-beta": ANTHROPIC_MCP_BETA,
        "content-type": "application/json",
    }
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]

    payload = {
        "model": settings.ANTHROPIC_MODEL,
        "max_tokens": settings.ANTHROPIC_MAX_TOKENS,
        "mcp_servers": [
            {
                "type": "url",
                "url": target.mcp_url,
                "name": "llmwiki",
                "authorization_token": target.mcp_auth_token,
            }
        ],
        "tools": [{"type": "mcp_toolset", "mcp_server_name": "llmwiki"}],
    }

    timeout = httpx.Timeout(settings.LLMWIKI_COMPILE_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for _ in range(settings.LLMWIKI_COMPILE_MAX_CONTINUATIONS + 1):
            response = await client.post(
                ANTHROPIC_API_URL,
                headers=headers,
                json=payload | {"messages": messages},
            )
            response.raise_for_status()

            data = response.json()
            stop_reason = data.get("stop_reason")
            if stop_reason == "pause_turn":
                messages.append({"role": "assistant", "content": data.get("content", [])})
                continue
            if stop_reason not in SUCCESS_STOP_REASONS:
                raise RuntimeError(f"Claude compile did not complete successfully (stop_reason={stop_reason})")
            break
        else:
            raise RuntimeError("Claude compile exceeded continuation limit")

    text_excerpt = "\n".join(
        block.get("text", "")
        for block in data.get("content", [])
        if block.get("type") == "text" and block.get("text")
    )[:2000]

    return {
        "stop_reason": stop_reason or "unknown",
        "request_id": data.get("id", ""),
        "text_excerpt": text_excerpt,
    }


async def _record_run(
    conn: asyncpg.Connection,
    knowledge_base_id: str,
    user_id: str,
    status: str,
    *,
    model: str,
    sources: list[dict[str, Any]],
    response_excerpt: str | None = None,
) -> str:
    row = await conn.fetchrow(
        "INSERT INTO compile_runs "
        "(knowledge_base_id, user_id, status, model, source_count, source_snapshot, response_excerpt, started_at, finished_at) "
        "VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb, $7, now(), CASE WHEN $3 = 'running' THEN NULL ELSE now() END) "
        "RETURNING id::text AS id",
        knowledge_base_id,
        user_id,
        status,
        model,
        len(sources),
        json.dumps(sources),
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
        "UPDATE compile_runs "
        "SET status = $1, response_excerpt = COALESCE($2, response_excerpt), error_message = $3, finished_at = now() "
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
        [
            (knowledge_base_id, source.id, source.version, run_id)
            for source in sources
        ],
    )
