import re
import secrets
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from config import settings
from deps import get_scoped_db, get_user_id
from scoped_db import ScopedDB
from services.encryption import decrypt_secret, encrypt_secret
from services.kb_access import ADMIN_ROLES, get_kb_for_member, require_kb_role
from services.periodic_compile import (
    CompileTarget,
    default_compile_provider,
    default_max_sources,
    default_max_tokens,
    default_max_tool_rounds,
    default_model_for_provider,
    get_compile_context,
    next_run_at,
    run_target,
)

router = APIRouter(prefix="/v1/knowledge-bases", tags=["knowledge-bases"])

_KB_COLUMNS = "id, user_id, name, slug, description, created_at, updated_at"
_KB_WITH_COUNTS = (
    "SELECT kb.id, kb.user_id, kb.name, kb.slug, kb.description, m.role, "
    "  COALESCE(s.wiki_direct_editing_enabled, false) AS wiki_direct_editing_enabled, "
    "  kb.created_at, kb.updated_at, "
    "  (SELECT COUNT(*) FROM documents d "
    "   WHERE d.knowledge_base_id = kb.id AND d.path NOT LIKE '/wiki/%%' AND NOT d.archived) AS source_count, "
    "  (SELECT COUNT(*) FROM documents d "
    "   WHERE d.knowledge_base_id = kb.id AND d.path LIKE '/wiki/%%' AND NOT d.archived) AS wiki_page_count "
    "FROM knowledge_bases kb "
    "JOIN knowledge_base_memberships m ON m.knowledge_base_id = kb.id"
    " LEFT JOIN knowledge_base_settings s ON s.knowledge_base_id = kb.id"
)


class CreateKnowledgeBase(BaseModel):
    name: str
    description: str | None = None


class UpdateKnowledgeBase(BaseModel):
    name: str | None = None
    description: str | None = None


class InviteCreate(BaseModel):
    email: str
    role: str


class MembershipUpdate(BaseModel):
    role: str


class UpdateCompileSchedule(BaseModel):
    enabled: bool
    provider: str
    model: str | None = None
    interval_minutes: int
    max_sources: int
    prompt: str | None = None
    provider_secret: str | None = None
    max_tool_rounds: int | None = None
    max_tokens: int | None = None
    wiki_direct_editing_enabled: bool | None = None


class KnowledgeBaseOut(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    slug: str
    role: str
    wiki_direct_editing_enabled: bool = False
    description: str | None = None
    source_count: int = 0
    wiki_page_count: int = 0
    created_at: datetime
    updated_at: datetime


class MembershipOut(BaseModel):
    user_id: UUID
    email: str | None = None
    display_name: str | None = None
    role: str
    created_at: datetime


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
    provider: str
    source_count: int
    response_excerpt: str | None = None
    error_message: str | None = None
    started_at: datetime
    finished_at: datetime | None = None


class KnowledgeBaseSettingsOut(BaseModel):
    knowledge_base: str
    enabled: bool
    provider: str
    model: str | None = None
    wiki_direct_editing_enabled: bool = False
    interval_minutes: int
    max_sources: int
    prompt: str = ""
    max_tool_rounds: int
    max_tokens: int
    has_provider_secret: bool
    last_run_at: datetime | None = None
    last_status: str | None = None
    last_error: str | None = None
    next_run_at: datetime | None = None


def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug).strip("-")
    return slug or "kb"


async def _unique_slug(pool, name: str) -> str:
    slug = _slugify(name)
    exists = await pool.fetchval("SELECT 1 FROM knowledge_bases WHERE slug = $1", slug)
    if exists:
        slug = f"{slug}-{secrets.token_hex(3)}"
    return slug


def _normalize_invite_email(email: str) -> str:
    return email.strip().lower()


def _resolved_max_sources(value: int | None) -> int:
    return value or default_max_sources()


def _resolved_max_tool_rounds(value: int | None) -> int:
    return value or default_max_tool_rounds()


def _resolved_max_tokens(value: int | None) -> int:
    return value or default_max_tokens()


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


@router.get("", response_model=list[KnowledgeBaseOut])
async def list_knowledge_bases(db: Annotated[ScopedDB, Depends(get_scoped_db)]):
    rows = await db.fetch(f"{_KB_WITH_COUNTS} WHERE m.user_id = $1 ORDER BY kb.updated_at DESC", db.user_id)
    return rows


@router.get("/{kb_id}", response_model=KnowledgeBaseOut)
async def get_knowledge_base(
    kb_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(f"{_KB_WITH_COUNTS} WHERE kb.id = $1 AND m.user_id = $2", kb_id, db.user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    return row


@router.get("/invites/pending", response_model=list[dict])
async def list_pending_knowledge_base_invites(
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    return []


@router.post("/invites/accept", response_model=KnowledgeBaseOut)
async def accept_knowledge_base_invite(
    body: dict,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    raise HTTPException(
        status_code=410,
        detail="Invite acceptance is no longer used. KB admins now add existing users directly by email.",
    )


@router.get("/{kb_id}/members", response_model=list[MembershipOut])
async def list_knowledge_base_members(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    kb = await get_kb_for_member(pool, str(kb_id), user_id)
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    rows = await pool.fetch(
        "SELECT m.user_id, u.email, u.display_name, m.role, m.created_at "
        "FROM knowledge_base_memberships m "
        "JOIN users u ON u.id = m.user_id "
        "WHERE m.knowledge_base_id = $1 ORDER BY m.created_at",
        kb_id,
    )
    return [dict(row) for row in rows]


@router.patch("/{kb_id}/members/{member_id}", response_model=MembershipOut)
async def update_knowledge_base_member(
    kb_id: UUID,
    member_id: UUID,
    body: MembershipUpdate,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    if body.role not in {"viewer", "editor", "admin"}:
        raise HTTPException(status_code=400, detail="Invalid member role")
    pool = request.app.state.pool
    try:
        access = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    if str(member_id) == access["owner_user_id"]:
        raise HTTPException(status_code=400, detail="Cannot change the owner role")
    row = await pool.fetchrow(
        "UPDATE knowledge_base_memberships m "
        "SET role = $1, updated_at = now() "
        "FROM users u "
        "WHERE m.knowledge_base_id = $2 AND m.user_id = $3 AND u.id = m.user_id "
        "RETURNING m.user_id, u.email, u.display_name, m.role, m.created_at",
        body.role,
        kb_id,
        member_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Member not found")
    return dict(row)


@router.delete("/{kb_id}/members/{member_id}", status_code=204)
async def remove_knowledge_base_member(
    kb_id: UUID,
    member_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        access = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    if str(member_id) == access["owner_user_id"]:
        raise HTTPException(status_code=400, detail="Cannot remove the owner from the knowledge base")
    result = await pool.execute(
        "DELETE FROM knowledge_base_memberships WHERE knowledge_base_id = $1 AND user_id = $2",
        kb_id,
        member_id,
    )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Member not found")


@router.get("/{kb_id}/invites", response_model=list[dict])
async def list_knowledge_base_invites(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    return []


@router.post("/{kb_id}/invites", response_model=MembershipOut, status_code=201)
async def create_knowledge_base_invite(
    kb_id: UUID,
    body: InviteCreate,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    if body.role not in {"viewer", "editor", "admin"}:
        raise HTTPException(status_code=400, detail="Invalid invite role")
    pool = request.app.state.pool
    try:
        await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    email = _normalize_invite_email(body.email)
    existing_member = await pool.fetchval(
        "SELECT 1 "
        "FROM knowledge_base_memberships m "
        "JOIN users u ON u.id = m.user_id "
        "WHERE m.knowledge_base_id = $1 AND lower(u.email) = $2",
        kb_id,
        email,
    )
    if existing_member:
        raise HTTPException(status_code=409, detail="That user already has access to this knowledge base")
    existing_user = await pool.fetchrow(
        "SELECT id, email, display_name FROM users WHERE lower(email) = $1",
        email,
    )
    if not existing_user:
        raise HTTPException(
            status_code=404,
            detail="That email does not belong to an existing user yet. Ask them to sign up first.",
        )
    row = await pool.fetchrow(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (knowledge_base_id, user_id) DO UPDATE SET role = EXCLUDED.role, updated_at = now() "
        "RETURNING user_id, role, created_at",
        kb_id,
        existing_user["id"],
        body.role,
    )
    return {
        **dict(row),
        "email": existing_user["email"],
        "display_name": existing_user["display_name"],
    }


@router.get("/{kb_id}/compile-preview", response_model=CompilePreviewOut)
async def get_compile_preview(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        kb = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    settings_row = await pool.fetchrow(
        "SELECT compile_max_sources FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    _, pending = await get_compile_context(
        pool,
        kb["slug"],
        settings_row["compile_max_sources"] if settings_row else settings.LLMWIKI_COMPILE_MAX_SOURCES,
    )
    return {"knowledge_base": kb["slug"], "pending_source_count": len(pending)}


@router.get("/{kb_id}/compile-runs", response_model=list[CompileRunOut])
async def list_compile_runs(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
    limit: int = Query(10, ge=1, le=50),
):
    pool = request.app.state.pool
    try:
        await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    rows = await pool.fetch(
        "SELECT id, status, model, provider, source_count, response_excerpt, error_message, started_at, finished_at "
        "FROM compile_runs WHERE knowledge_base_id = $1 ORDER BY started_at DESC LIMIT $2",
        kb_id,
        limit,
    )
    return [dict(row) for row in rows]


@router.get("/{kb_id}/compile-schedule", response_model=KnowledgeBaseSettingsOut)
async def get_compile_schedule(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        kb = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    row = await pool.fetchrow(
        "SELECT auto_compile_enabled AS enabled, compile_provider AS provider, compile_model AS model, "
        "compile_interval_minutes AS interval_minutes, compile_max_sources AS max_sources, compile_prompt AS prompt, "
        "compile_max_tool_rounds AS max_tool_rounds, compile_max_tokens AS max_tokens, "
        "COALESCE(wiki_direct_editing_enabled, false) AS wiki_direct_editing_enabled, "
        "(provider_secret_encrypted IS NOT NULL) AS has_provider_secret, "
        "last_run_at, last_status, last_error, next_run_at "
        "FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    if not row:
        provider = default_compile_provider()
        return {
            "knowledge_base": kb["slug"],
            "enabled": False,
            "provider": provider,
            "model": default_model_for_provider(provider) or None,
            "wiki_direct_editing_enabled": False,
            "interval_minutes": 60,
            "max_sources": default_max_sources(),
            "prompt": "",
            "max_tool_rounds": default_max_tool_rounds(),
            "max_tokens": default_max_tokens(),
            "has_provider_secret": False,
        }
    return {"knowledge_base": kb["slug"], **dict(row)}


@router.put("/{kb_id}/compile-schedule", response_model=KnowledgeBaseSettingsOut)
async def update_compile_schedule(
    kb_id: UUID,
    body: UpdateCompileSchedule,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        kb = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    provider = body.provider.strip().lower()
    if provider not in {"anthropic", "openrouter"}:
        raise HTTPException(status_code=400, detail="Unsupported compile provider")
    model = (body.model or "").strip() or default_model_for_provider(provider)
    if not model:
        raise HTTPException(status_code=400, detail="Model is required")
    max_sources = _resolved_max_sources(body.max_sources)
    max_tool_rounds = _resolved_max_tool_rounds(body.max_tool_rounds)
    max_tokens = _resolved_max_tokens(body.max_tokens)
    if body.interval_minutes < 5 or body.interval_minutes > 10080:
        raise HTTPException(status_code=400, detail="Interval must be between 5 minutes and 7 days")
    if max_sources < 1 or max_sources > 200:
        raise HTTPException(status_code=400, detail="Max sources must be between 1 and 200")
    if max_tool_rounds < 1 or max_tool_rounds > 500:
        raise HTTPException(status_code=400, detail="Max tool rounds must be between 1 and 500")
    if max_tokens < 256 or max_tokens > 200000:
        raise HTTPException(status_code=400, detail="Max tokens must be between 256 and 200000")
    existing_secret = await pool.fetchval(
        "SELECT provider_secret_encrypted FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    existing_direct_editing = await pool.fetchval(
        "SELECT wiki_direct_editing_enabled FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    provider_secret = (body.provider_secret or "").strip()
    if body.enabled and not provider_secret and not existing_secret:
        raise HTTPException(status_code=400, detail="Provider secret is required before enabling periodic compile")
    encrypted_secret = encrypt_secret(provider_secret) if provider_secret else None
    next_due = next_run_at(body.interval_minutes) if body.enabled else None
    wiki_direct_editing_enabled = (
        body.wiki_direct_editing_enabled
        if body.wiki_direct_editing_enabled is not None
        else bool(existing_direct_editing)
    )
    row = await pool.fetchrow(
        "INSERT INTO knowledge_base_settings "
        "(knowledge_base_id, auto_compile_enabled, compile_provider, compile_model, compile_interval_minutes, compile_max_sources, compile_prompt, compile_max_tool_rounds, compile_max_tokens, wiki_direct_editing_enabled, provider_secret_encrypted, provider_secret_updated_at, next_run_at, updated_by) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::text, CASE WHEN $11::text IS NULL THEN NULL ELSE now() END, $12, $13) "
        "ON CONFLICT (knowledge_base_id) DO UPDATE SET "
        "auto_compile_enabled = EXCLUDED.auto_compile_enabled, compile_provider = EXCLUDED.compile_provider, compile_model = EXCLUDED.compile_model, "
        "compile_interval_minutes = EXCLUDED.compile_interval_minutes, compile_max_sources = EXCLUDED.compile_max_sources, compile_prompt = EXCLUDED.compile_prompt, "
        "compile_max_tool_rounds = EXCLUDED.compile_max_tool_rounds, compile_max_tokens = EXCLUDED.compile_max_tokens, "
        "wiki_direct_editing_enabled = EXCLUDED.wiki_direct_editing_enabled, "
        "provider_secret_encrypted = COALESCE(EXCLUDED.provider_secret_encrypted, knowledge_base_settings.provider_secret_encrypted), "
        "provider_secret_updated_at = CASE WHEN EXCLUDED.provider_secret_encrypted IS NULL THEN knowledge_base_settings.provider_secret_updated_at ELSE now() END, "
        "next_run_at = EXCLUDED.next_run_at, updated_by = EXCLUDED.updated_by "
        "RETURNING auto_compile_enabled AS enabled, compile_provider AS provider, compile_model AS model, "
        "compile_interval_minutes AS interval_minutes, compile_max_sources AS max_sources, compile_prompt AS prompt, "
        "compile_max_tool_rounds AS max_tool_rounds, compile_max_tokens AS max_tokens, "
        "COALESCE(wiki_direct_editing_enabled, false) AS wiki_direct_editing_enabled, "
        "(provider_secret_encrypted IS NOT NULL) AS has_provider_secret, "
        "last_run_at, last_status, last_error, next_run_at",
        kb_id,
        body.enabled,
        provider,
        model,
        body.interval_minutes,
        max_sources,
        (body.prompt or "").strip(),
        max_tool_rounds,
        max_tokens,
        wiki_direct_editing_enabled,
        encrypted_secret,
        next_due,
        user_id,
    )
    return {"knowledge_base": kb["slug"], **dict(row)}


@router.post("/{kb_id}/compile-now", response_model=CompileNowOut)
async def compile_knowledge_base_now(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        kb = await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    settings_row = await pool.fetchrow(
        "SELECT compile_provider, compile_model, compile_max_sources, compile_prompt, "
        "compile_max_tool_rounds, compile_max_tokens, provider_secret_encrypted "
        "FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    if not settings_row or not settings_row["provider_secret_encrypted"]:
        raise HTTPException(status_code=400, detail="Compile provider secret is not configured")
    target = CompileTarget(
        knowledge_base=kb["slug"],
        provider_api_key=decrypt_secret(settings_row["provider_secret_encrypted"]) or "",
        prompt=settings_row["compile_prompt"] or "",
        max_sources=settings_row["compile_max_sources"] or default_max_sources(),
        provider=settings_row["compile_provider"],
        model=settings_row["compile_model"] or default_model_for_provider(settings_row["compile_provider"]),
        max_tool_rounds=settings_row["compile_max_tool_rounds"] or default_max_tool_rounds(),
        max_tokens=settings_row["compile_max_tokens"] or default_max_tokens(),
        actor_user_id=kb["owner_user_id"],
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

    existing = await pool.fetchval(
        "SELECT 1 FROM knowledge_bases WHERE user_id = $1 AND name = $2",
        user_id,
        body.name,
    )
    if existing:
        raise HTTPException(status_code=409, detail="You already have a knowledge base with that name.")

    slug = await _unique_slug(pool, body.name)

    conn = await pool.acquire()
    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"INSERT INTO knowledge_bases (user_id, name, slug, description) VALUES ($1, $2, $3, $4) RETURNING {_KB_COLUMNS}",
                user_id,
                body.name,
                slug,
                body.description,
            )
            kb_id = row["id"]
            today = datetime.now(UTC).strftime("%Y-%m-%d")

            await conn.execute(
                "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, file_type, status, content, tags, version, sort_order) "
                "VALUES ($1, $2, 'overview.md', 'Overview', '/wiki/', 'md', 'ready', $3, $4, 0, -100)",
                kb_id,
                user_id,
                _OVERVIEW_TEMPLATE.format(name=body.name),
                ["overview"],
            )
            await conn.execute(
                "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, file_type, status, content, tags, version, sort_order) "
                "VALUES ($1, $2, 'log.md', 'Log', '/wiki/', 'md', 'ready', $3, $4, 0, 100)",
                kb_id,
                user_id,
                _LOG_TEMPLATE.format(name=body.name, date=today),
                ["log"],
            )
            await conn.execute(
                "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner') "
                "ON CONFLICT (knowledge_base_id, user_id) DO NOTHING",
                kb_id,
                user_id,
            )
            await conn.execute(
                "INSERT INTO knowledge_base_settings (knowledge_base_id, updated_by) VALUES ($1, $2) "
                "ON CONFLICT (knowledge_base_id) DO NOTHING",
                kb_id,
                user_id,
            )
            row = await conn.fetchrow(f"{_KB_WITH_COUNTS} WHERE kb.id = $1 AND m.user_id = $2", kb_id, user_id)
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
    try:
        await require_kb_role(pool, str(kb_id), user_id, *ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc

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
    sql = f"UPDATE knowledge_bases SET {', '.join(updates)} WHERE id = ${idx} RETURNING id"
    row = await pool.fetchrow(sql, *params)
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    full_row = await pool.fetchrow(f"{_KB_WITH_COUNTS} WHERE kb.id = $1 AND m.user_id = $2", kb_id, user_id)
    return dict(full_row)


@router.delete("/{kb_id}", status_code=204)
async def delete_knowledge_base(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    pool = request.app.state.pool
    try:
        await require_kb_role(pool, str(kb_id), user_id, "owner")
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    result = await pool.execute("DELETE FROM knowledge_bases WHERE id = $1", kb_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Knowledge base not found")
