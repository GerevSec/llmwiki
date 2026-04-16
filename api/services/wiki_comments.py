from __future__ import annotations

from typing import Any

import asyncpg

_COMMENT_COLUMNS = (
    "id, kb_id, scope_page_key, body, status, failure_reason, system_note, author_id, "
    "created_at, compiled_at, compiled_run_id, resolved_at, resolved_by, "
    "promoted_to_directive_id"
)

# Admin-initiated manual transitions only.
# Auto transitions (open→resolved, open→failed) are handled by the compile pipeline.
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "open":     {"archived"},
    "failed":   {"archived"},
    "resolved": {"archived"},
}


async def list_page_comments(
    pool: asyncpg.Pool,
    kb_id: str,
    page_key: str,
) -> list[dict[str, Any]]:
    """Fetch non-archived comments for a page with lazy orphan archival.

    This function performs a lazy archival UPDATE and must be served from primary DB only.
    """
    active_release_id = await pool.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        kb_id,
    )

    rows = await pool.fetch(
        f"SELECT {_COMMENT_COLUMNS} FROM kb_directives "
        "WHERE kb_id = $1::uuid AND kind = 'comment' AND scope_page_key = $2::uuid AND status != 'archived' "
        "ORDER BY created_at ASC",
        kb_id,
        page_key,
    )

    if active_release_id:
        page_exists = await pool.fetchval(
            "SELECT 1 FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid",
            active_release_id,
            page_key,
        )
        if not page_exists:
            orphan_ids = [row["id"] for row in rows if row["status"] == "open"]
            if orphan_ids:
                # MVCC race-safe: status='open' predicate prevents double-archival
                await pool.execute(
                    "UPDATE kb_directives "
                    "SET status = 'archived', archived_at = now(), system_note = 'orphaned', updated_at = now() "
                    "WHERE id = ANY($1::uuid[]) AND kind = 'comment' AND status = 'open'",
                    orphan_ids,
                )
                rows = await pool.fetch(
                    f"SELECT {_COMMENT_COLUMNS} FROM kb_directives "
                    "WHERE kb_id = $1::uuid AND kind = 'comment' AND scope_page_key = $2::uuid AND status != 'archived' "
                    "ORDER BY created_at ASC",
                    kb_id,
                    page_key,
                )

    return [dict(row) for row in rows]


async def create_comment(
    pool: asyncpg.Pool,
    kb_id: str,
    page_key: str,
    body: str,
    author_id: str,
) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"INSERT INTO kb_directives (kb_id, kind, scope_page_key, body, status, author_id) "
        f"VALUES ($1::uuid, 'comment', $2::uuid, $3, 'open', $4::uuid) "
        f"RETURNING {_COMMENT_COLUMNS}",
        kb_id,
        page_key,
        body,
        author_id,
    )
    # Pull the next compile forward so newly-filed comments don't wait a full interval.
    await pool.execute(
        "UPDATE knowledge_base_settings "
        "SET next_run_at = LEAST(COALESCE(next_run_at, now() + interval '30 seconds'), now() + interval '30 seconds') "
        "WHERE knowledge_base_id = $1::uuid",
        kb_id,
    )
    return dict(row)  # type: ignore[arg-type]


class IllegalTransitionError(Exception):
    pass


async def transition_comment(
    pool: asyncpg.Pool,
    comment_id: str,
    new_status: str,
    actor_id: str,
) -> dict[str, Any] | None:
    """Transition a comment status via admin action. Raises IllegalTransitionError on invalid moves.

    Allowed HTTP transitions (all admin-initiated):
      open     → archived  (suppresses comment before next compile)
      failed   → archived  (suppresses a failed comment)
      resolved → archived  (bookkeeping; does not change wiki content)

    Auto transitions (open→resolved, open→failed) are handled by the compile pipeline.
    """
    row = await pool.fetchrow(
        "SELECT id, status FROM kb_directives WHERE id = $1::uuid AND kind = 'comment'",
        comment_id,
    )
    if not row:
        return None

    current = row["status"]
    allowed = _ALLOWED_TRANSITIONS.get(current, set())
    if new_status not in allowed:
        raise IllegalTransitionError(
            f"Cannot transition comment from '{current}' to '{new_status}'"
        )

    updated = await pool.fetchrow(
        f"UPDATE kb_directives "
        f"SET status = 'archived', archived_at = now(), updated_at = now() "
        f"WHERE id = $1::uuid AND kind = 'comment' AND status = $2 "
        f"RETURNING {_COMMENT_COLUMNS}",
        comment_id,
        current,
    )
    return dict(updated) if updated else None


async def promote_comment(
    pool: asyncpg.Pool,
    comment_id: str,
    guideline_body: str | None,
    actor_id: str,
) -> dict[str, Any] | None:
    """Promote a comment to a KB guideline (orthogonal to comment status).

    Any comment in any status may be promoted. Creates a new kind='guideline' row
    and sets promoted_to_directive_id on the comment. Comment status is unchanged.
    """
    row = await pool.fetchrow(
        "SELECT id, kb_id, body FROM kb_directives WHERE id = $1::uuid AND kind = 'comment'",
        comment_id,
    )
    if not row:
        return None

    kb_id = str(row["kb_id"])
    body = guideline_body or row["body"]

    conn = await pool.acquire()
    try:
        async with conn.transaction():
            guideline = await conn.fetchrow(
                "INSERT INTO kb_directives (kb_id, kind, body, position, author_id) "
                "VALUES ($1::uuid, 'guideline', $2, "
                "  (SELECT COALESCE(MAX(position), 0) + 1 FROM kb_directives "
                "   WHERE kb_id = $1::uuid AND kind = 'guideline' AND archived_at IS NULL), "
                "  $3::uuid) "
                "RETURNING id",
                kb_id,
                body,
                actor_id,
            )
            await conn.execute(
                "UPDATE kb_directives "
                "SET promoted_to_directive_id = $1::uuid, updated_at = now() "
                "WHERE id = $2::uuid AND kind = 'comment'",
                str(guideline["id"]),
                comment_id,
            )
    finally:
        await pool.release(conn)

    updated = await pool.fetchrow(
        f"SELECT {_COMMENT_COLUMNS} FROM kb_directives WHERE id = $1::uuid AND kind = 'comment'",
        comment_id,
    )
    return dict(updated) if updated else None
