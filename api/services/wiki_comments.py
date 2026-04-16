from __future__ import annotations

from typing import Any

import asyncpg

_COMMENT_COLUMNS = (
    "id, kb_id, page_key, body, status, system_note, author_id, "
    "created_at, delivered_at, delivered_compile_id, resolved_at, resolved_by, "
    "promoted_to_guideline_id"
)

# Allowed HTTP-initiated transitions (open→archived is system-only via lazy orphan path)
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "delivered": {"resolved", "archived"},
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
        f"SELECT {_COMMENT_COLUMNS} FROM wiki_page_comments "
        "WHERE kb_id = $1::uuid AND page_key = $2::uuid AND status != 'archived' "
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
                    "UPDATE wiki_page_comments "
                    "SET status = 'archived', resolved_by = NULL, system_note = 'orphaned', resolved_at = NOW() "
                    "WHERE id = ANY($1::uuid[]) AND status = 'open'",
                    orphan_ids,
                )
                rows = await pool.fetch(
                    f"SELECT {_COMMENT_COLUMNS} FROM wiki_page_comments "
                    "WHERE kb_id = $1::uuid AND page_key = $2::uuid AND status != 'archived' "
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
        f"INSERT INTO wiki_page_comments (kb_id, page_key, body, author_id) "
        f"VALUES ($1::uuid, $2::uuid, $3, $4::uuid) "
        f"RETURNING {_COMMENT_COLUMNS}",
        kb_id,
        page_key,
        body,
        author_id,
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
    """Transition a comment status. Raises IllegalTransitionError on invalid moves.

    Allowed HTTP transitions:
      delivered → resolved  (sets resolved_at + resolved_by)
      delivered → archived  (sets resolved_at)

    open → archived is system-only (lazy orphan path); NOT callable here.
    """
    row = await pool.fetchrow(
        "SELECT id, status FROM wiki_page_comments WHERE id = $1::uuid",
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

    if new_status == "resolved":
        updated = await pool.fetchrow(
            f"UPDATE wiki_page_comments "
            f"SET status = 'resolved', resolved_at = NOW(), resolved_by = $1::uuid "
            f"WHERE id = $2::uuid AND status = 'delivered' "
            f"RETURNING {_COMMENT_COLUMNS}",
            actor_id,
            comment_id,
        )
    else:  # archived
        updated = await pool.fetchrow(
            f"UPDATE wiki_page_comments "
            f"SET status = 'archived', resolved_at = NOW() "
            f"WHERE id = $1::uuid AND status = 'delivered' "
            f"RETURNING {_COMMENT_COLUMNS}",
            comment_id,
        )
    return dict(updated) if updated else None


async def promote_comment(
    pool: asyncpg.Pool,
    comment_id: str,
    guideline_body: str | None,
    actor_id: str,
) -> dict[str, Any] | None:
    """Promote a delivered comment to a KB guideline in a single transaction."""
    row = await pool.fetchrow(
        "SELECT id, kb_id, body, status FROM wiki_page_comments WHERE id = $1::uuid",
        comment_id,
    )
    if not row:
        return None
    if row["status"] != "delivered":
        raise IllegalTransitionError(
            f"Cannot promote comment with status '{row['status']}'; only 'delivered' comments may be promoted"
        )

    kb_id = str(row["kb_id"])
    body = guideline_body or row["body"]

    conn = await pool.acquire()
    try:
        async with conn.transaction():
            guideline = await conn.fetchrow(
                "INSERT INTO kb_guidelines (kb_id, body, position, created_by) "
                "VALUES ($1::uuid, $2, "
                "  (SELECT COALESCE(MAX(position), 0) + 1 FROM kb_guidelines WHERE kb_id = $1::uuid AND archived_at IS NULL), "
                "  $3::uuid) "
                "RETURNING id",
                kb_id,
                body,
                actor_id,
            )
            await conn.execute(
                "UPDATE wiki_page_comments "
                "SET status = 'promoted', promoted_to_guideline_id = $1::uuid "
                "WHERE id = $2::uuid AND status = 'delivered'",
                str(guideline["id"]),
                comment_id,
            )
    finally:
        await pool.release(conn)

    updated = await pool.fetchrow(
        f"SELECT {_COMMENT_COLUMNS} FROM wiki_page_comments WHERE id = $1::uuid",
        comment_id,
    )
    return dict(updated) if updated else None
