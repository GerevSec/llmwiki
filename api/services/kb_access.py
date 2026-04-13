from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable
from uuid import UUID

import asyncpg

READ_ROLES = ("viewer", "editor", "admin", "owner")
EDIT_ROLES = ("editor", "admin", "owner")
ADMIN_ROLES = ("admin", "owner")


@dataclass(frozen=True)
class KBAccess:
    id: str
    slug: str
    name: str
    role: str
    owner_user_id: str


def is_wiki_path(path: str | None) -> bool:
    normalized = (path or "/").strip()
    if not normalized:
        normalized = "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if not normalized.endswith("/"):
        normalized += "/"
    normalized = re.sub(r"/+", "/", normalized)
    return normalized == "/wiki/" or normalized.startswith("/wiki/")


async def wiki_direct_editing_enabled(pool: asyncpg.Pool, kb_id: str) -> bool:
    enabled = await pool.fetchval(
        "SELECT COALESCE(wiki_direct_editing_enabled, false) FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        kb_id,
    )
    return bool(enabled)


async def resolve_kb_access(
    pool: asyncpg.Pool,
    user_id: str,
    kb_id_or_slug: str,
    allowed_roles: Iterable[str] = READ_ROLES,
) -> KBAccess | None:
    try:
        UUID(kb_id_or_slug)
        identifier_column = "kb.id::text"
    except (ValueError, TypeError):
        identifier_column = "kb.slug"
    row = await pool.fetchrow(
        "SELECT kb.id::text AS id, kb.slug, kb.name, kb.user_id::text AS owner_user_id, m.role "
        "FROM knowledge_bases kb "
        "JOIN knowledge_base_memberships m ON m.knowledge_base_id = kb.id "
        f"WHERE {identifier_column} = $1 AND m.user_id = $2",
        kb_id_or_slug,
        user_id,
    )
    if not row:
        return None
    if row["role"] not in tuple(allowed_roles):
        return None
    return KBAccess(**dict(row))


async def require_kb_access(
    pool: asyncpg.Pool,
    user_id: str,
    kb_id_or_slug: str,
    allowed_roles: Iterable[str] = READ_ROLES,
) -> KBAccess:
    access = await resolve_kb_access(pool, user_id, kb_id_or_slug, allowed_roles)
    if not access:
        raise PermissionError("Knowledge base not found or not permitted")
    return access


async def get_document_access(
    pool: asyncpg.Pool,
    user_id: str,
    doc_id: str,
    allowed_roles: Iterable[str] = READ_ROLES,
) -> dict | None:
    row = await pool.fetchrow(
        "SELECT d.id::text AS id, d.knowledge_base_id::text AS knowledge_base_id, d.user_id::text AS user_id, "
        "d.filename, d.path, d.title, d.file_type, d.content, d.archived, m.role, kb.slug, kb.name "
        "FROM documents d "
        "JOIN knowledge_base_memberships m ON m.knowledge_base_id = d.knowledge_base_id "
        "JOIN knowledge_bases kb ON kb.id = d.knowledge_base_id "
        "WHERE d.id = $1::uuid AND m.user_id = $2 AND NOT d.archived",
        doc_id,
        user_id,
    )
    if not row or row["role"] not in tuple(allowed_roles):
        return None
    return dict(row)


async def get_user_email(pool: asyncpg.Pool, user_id: str) -> str | None:
    return await pool.fetchval("SELECT email FROM users WHERE id = $1", user_id)


async def get_kb_for_member(pool: asyncpg.Pool, kb_id_or_slug: str, user_id: str) -> dict | None:
    access = await resolve_kb_access(pool, user_id, kb_id_or_slug, READ_ROLES)
    return access.__dict__ if access else None


async def get_kb_membership(pool: asyncpg.Pool, kb_id: str, user_id: str) -> str | None:
    access = await resolve_kb_access(pool, user_id, kb_id, READ_ROLES)
    return access.role if access else None


async def require_kb_role(pool: asyncpg.Pool, kb_id_or_slug: str, user_id: str, *roles: str) -> dict:
    allowed_roles = roles or READ_ROLES
    access = await require_kb_access(pool, user_id, kb_id_or_slug, allowed_roles)
    return access.__dict__
