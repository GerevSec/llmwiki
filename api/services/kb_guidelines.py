from __future__ import annotations

from typing import Any

import asyncpg


async def render_guidelines_block(pool: asyncpg.Pool, kb_id: str) -> str:
    rows = await pool.fetch(
        "SELECT body FROM kb_guidelines WHERE kb_id = $1::uuid AND is_active = true AND archived_at IS NULL ORDER BY position ASC",
        kb_id,
    )
    if not rows:
        return ""
    bullets = "\n".join(f"- {row['body']}" for row in rows)
    return f"<kb_guidelines>\n{bullets}\n</kb_guidelines>"


async def list_guidelines(pool: asyncpg.Pool, kb_id: str) -> list[dict[str, Any]]:
    rows = await pool.fetch(
        "SELECT id, kb_id, body, position, is_active, created_by, created_at, updated_at, archived_at "
        "FROM kb_guidelines WHERE kb_id = $1::uuid AND archived_at IS NULL ORDER BY position ASC",
        kb_id,
    )
    return [dict(row) for row in rows]


async def create_guideline(
    pool: asyncpg.Pool,
    kb_id: str,
    body: str,
    created_by: str,
    position: int | None = None,
) -> dict[str, Any]:
    if position is None:
        max_pos = await pool.fetchval(
            "SELECT COALESCE(MAX(position), 0) FROM kb_guidelines WHERE kb_id = $1::uuid AND archived_at IS NULL",
            kb_id,
        )
        position = (max_pos or 0) + 1
    row = await pool.fetchrow(
        "INSERT INTO kb_guidelines (kb_id, body, position, created_by) "
        "VALUES ($1::uuid, $2, $3, $4::uuid) "
        "RETURNING id, kb_id, body, position, is_active, created_by, created_at, updated_at, archived_at",
        kb_id,
        body,
        position,
        created_by,
    )
    return dict(row)  # type: ignore[arg-type]


async def update_guideline(
    pool: asyncpg.Pool,
    guideline_id: str,
    *,
    body: str | None = None,
    position: int | None = None,
    is_active: bool | None = None,
) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    idx = 1
    if body is not None:
        updates.append(f"body = ${idx}")
        params.append(body)
        idx += 1
    if position is not None:
        updates.append(f"position = ${idx}")
        params.append(position)
        idx += 1
    if is_active is not None:
        updates.append(f"is_active = ${idx}")
        params.append(is_active)
        idx += 1
    if not updates:
        row = await pool.fetchrow(
            "SELECT id, kb_id, body, position, is_active, created_by, created_at, updated_at, archived_at "
            "FROM kb_guidelines WHERE id = $1::uuid AND archived_at IS NULL",
            guideline_id,
        )
        return dict(row) if row else None
    updates.append("updated_at = now()")
    params.append(guideline_id)
    row = await pool.fetchrow(
        f"UPDATE kb_guidelines SET {', '.join(updates)} WHERE id = ${idx}::uuid AND archived_at IS NULL "
        "RETURNING id, kb_id, body, position, is_active, created_by, created_at, updated_at, archived_at",
        *params,
    )
    return dict(row) if row else None


async def delete_guideline(pool: asyncpg.Pool, guideline_id: str) -> bool:
    result = await pool.execute(
        "UPDATE kb_guidelines SET archived_at = now(), updated_at = now() WHERE id = $1::uuid AND archived_at IS NULL",
        guideline_id,
    )
    return result != "UPDATE 0"
