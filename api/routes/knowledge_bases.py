import re
import secrets
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from config import settings
from deps import get_scoped_db, get_user_id
from scoped_db import ScopedDB
from services.periodic_compile import CompileTarget, filter_pending_sources, run_target

router = APIRouter(prefix="/v1/knowledge-bases", tags=["knowledge-bases"])

_KB_COLUMNS = "id, user_id, name, slug, description, created_at, updated_at"
_KB_WITH_COUNTS = (
    "SELECT kb.id, kb.user_id, kb.name, kb.slug, kb.description, "
    "  kb.created_at, kb.updated_at, "
    "  (SELECT COUNT(*) FROM documents d "
    "   WHERE d.knowledge_base_id = kb.id AND d.path NOT LIKE '/wiki/%%' AND NOT d.archived) AS source_count, "
    "  (SELECT COUNT(*) FROM documents d "
    "   WHERE d.knowledge_base_id = kb.id AND d.path LIKE '/wiki/%%' AND NOT d.archived) AS wiki_page_count "
    "FROM knowledge_bases kb"
)


class CreateKnowledgeBase(BaseModel):
    name: str
    description: str | None = None


class UpdateKnowledgeBase(BaseModel):
    name: str | None = None
    description: str | None = None


class KnowledgeBaseOut(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    slug: str
    description: str | None = None
    source_count: int = 0
    wiki_page_count: int = 0
    created_at: datetime
    updated_at: datetime


class CompileNowOut(BaseModel):
    knowledge_base: str
    status: str
    source_count: int
    stop_reason: str | None = None
    request_id: str | None = None


class CompilePreviewOut(BaseModel):
    knowledge_base: str
    pending_source_count: int


class CompileRunOut(BaseModel):
    id: UUID
    status: str
    model: str
    source_count: int
    response_excerpt: str | None = None
    error_message: str | None = None
    started_at: datetime
    finished_at: datetime | None = None


def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug).strip("-")
    return slug or "kb"


async def _unique_slug(pool, user_id: str, name: str) -> str:
    slug = _slugify(name)
    exists = await pool.fetchval(
        "SELECT 1 FROM knowledge_bases WHERE slug = $1 AND user_id = $2",
        slug, user_id,
    )
    if exists:
        slug = f"{slug}-{secrets.token_hex(3)}"
    return slug


_OVERVIEW_TEMPLATE = """\
This wiki tracks research on {name}. No sources have been ingested yet.

## Key Findings

No sources ingested yet — add your first source to get started.

## Recent Updates

No activity yet.\
"""

_LOG_TEMPLATE = """\
Chronological record of ingests, queries, and maintenance passes.

## [{date}] created | Wiki Created
- Initialized wiki: {name}\
"""


# ── Read routes (RLS-enforced via ScopedDB) ──

@router.get("", response_model=list[KnowledgeBaseOut])
async def list_knowledge_bases(db: Annotated[ScopedDB, Depends(get_scoped_db)]):
    rows = await db.fetch(f"{_KB_WITH_COUNTS} ORDER BY kb.updated_at DESC")
    return rows


@router.get("/{kb_id}", response_model=KnowledgeBaseOut)
async def get_knowledge_base(
    kb_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(f"{_KB_WITH_COUNTS} WHERE kb.id = $1", kb_id)
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    return row


@router.get("/{kb_id}/compile-preview", response_model=CompilePreviewOut)
async def get_compile_preview(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    kb = await pool.fetchrow(
        "SELECT id::text AS id, slug FROM knowledge_bases WHERE id = $1 AND user_id = $2",
        kb_id,
        user_id,
    )
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")

    checkpoint_rows = await pool.fetch(
        "SELECT document_id::text AS document_id, compiled_version "
        "FROM compiled_source_checkpoints WHERE knowledge_base_id = $1::uuid",
        kb["id"],
    )
    checkpoints = {row["document_id"]: row["compiled_version"] for row in checkpoint_rows}

    document_rows = [
        dict(row)
        for row in await pool.fetch(
            "SELECT id::text AS id, path, filename, COALESCE(title, filename) AS title, "
            "status, archived, version, updated_at "
            "FROM documents WHERE knowledge_base_id = $1::uuid",
            kb["id"],
        )
    ]
    pending = filter_pending_sources(
        document_rows,
        checkpoints,
        settings.LLMWIKI_COMPILE_MAX_SOURCES,
    )
    return {
        "knowledge_base": kb["slug"],
        "pending_source_count": len(pending),
    }


@router.get("/{kb_id}/compile-runs", response_model=list[CompileRunOut])
async def list_compile_runs(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
    limit: int = Query(10, ge=1, le=50),
):
    pool = request.app.state.pool
    kb = await pool.fetchrow(
        "SELECT id FROM knowledge_bases WHERE id = $1 AND user_id = $2",
        kb_id,
        user_id,
    )
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")

    rows = await pool.fetch(
        "SELECT id, status, model, source_count, response_excerpt, error_message, started_at, finished_at "
        "FROM compile_runs WHERE knowledge_base_id = $1 ORDER BY started_at DESC LIMIT $2",
        kb_id,
        limit,
    )
    return [dict(row) for row in rows]


@router.post("/{kb_id}/compile-now", response_model=CompileNowOut)
async def compile_knowledge_base_now(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    if not settings.ANTHROPIC_API_KEY or not settings.ANTHROPIC_MODEL:
        raise HTTPException(
            status_code=503,
            detail="Periodic compile is not configured on the server.",
        )

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization header")

    access_token = auth_header.removeprefix("Bearer ").strip()
    pool = request.app.state.pool
    kb = await pool.fetchrow(
        "SELECT slug FROM knowledge_bases WHERE id = $1 AND user_id = $2",
        kb_id,
        user_id,
    )
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")

    target = CompileTarget(
        knowledge_base=kb["slug"],
        mcp_auth_token=access_token,
        mcp_url=settings.MCP_URL,
        prompt=settings.LLMWIKI_COMPILE_PROMPT,
        max_sources=settings.LLMWIKI_COMPILE_MAX_SOURCES,
    )

    try:
        result = await run_target(pool, target)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return result


# ── Write routes (service role via pool) ──

@router.post("", response_model=KnowledgeBaseOut, status_code=201)
async def create_knowledge_base(
    body: CreateKnowledgeBase,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool

    user_count = await pool.fetchval("SELECT COUNT(DISTINCT id) FROM users")
    if user_count and user_count >= settings.GLOBAL_MAX_USERS:
        raise HTTPException(
            status_code=503,
            detail="We've reached our user capacity for now. Please try again later.",
        )

    slug = await _unique_slug(pool, user_id, body.name)

    conn = await pool.acquire()
    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"INSERT INTO knowledge_bases (user_id, name, slug, description) "
                f"VALUES ($1, $2, $3, $4) RETURNING {_KB_COLUMNS}",
                user_id, body.name, slug, body.description,
            )

            kb_id = row["id"]
            today = datetime.now().strftime("%Y-%m-%d")

            await conn.execute(
                "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, "
                "file_type, status, content, tags, version, sort_order) "
                "VALUES ($1, $2, 'overview.md', 'Overview', '/wiki/', "
                "'md', 'ready', $3, $4, 0, -100)",
                kb_id, user_id,
                _OVERVIEW_TEMPLATE.format(name=body.name),
                ["overview"],
            )

            await conn.execute(
                "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, "
                "file_type, status, content, tags, version, sort_order) "
                "VALUES ($1, $2, 'log.md', 'Log', '/wiki/', "
                "'md', 'ready', $3, $4, 0, 100)",
                kb_id, user_id,
                _LOG_TEMPLATE.format(name=body.name, date=today),
                ["log"],
            )
    finally:
        await pool.release(conn)

    return dict(row)


@router.patch("/{kb_id}", response_model=KnowledgeBaseOut)
async def update_knowledge_base(
    kb_id: UUID,
    body: UpdateKnowledgeBase,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool

    updates = []
    params = []
    idx = 1

    if body.name is not None:
        updates.append(f"name = ${idx}")
        params.append(body.name)
        idx += 1
    if body.description is not None:
        updates.append(f"description = ${idx}")
        params.append(body.description)
        idx += 1

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = now()")
    params.append(kb_id)
    params.append(user_id)

    sql = (
        f"UPDATE knowledge_bases SET {', '.join(updates)} "
        f"WHERE id = ${idx} AND user_id = ${idx + 1} "
        f"RETURNING {_KB_COLUMNS}"
    )
    row = await pool.fetchrow(sql, *params)
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    return dict(row)


@router.delete("/{kb_id}", status_code=204)
async def delete_knowledge_base(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    result = await pool.execute(
        "DELETE FROM knowledge_bases WHERE id = $1 AND user_id = $2",
        kb_id, user_id,
    )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Knowledge base not found")
