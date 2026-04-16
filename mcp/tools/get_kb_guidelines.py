from mcp.server.fastmcp import FastMCP, Context

from db import scoped_query
from .helpers import get_user_id, resolve_kb


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="get_kb_guidelines",
        description=(
            "Return active guidelines for a knowledge base as markdown bullets.\n\n"
            "Guidelines are standing rules authored by KB admins/owners that inform "
            "how the wiki should be compiled. Call this before `write` to understand "
            "the KB's editorial standards.\n\n"
            "Use `list_knowledge_bases` first to find available `kb_slug` values."
        ),
    )
    async def get_kb_guidelines(ctx: Context, kb_slug: str) -> str:
        user_id = get_user_id(ctx)
        kb = await resolve_kb(user_id, kb_slug)
        if not kb:
            return f"Knowledge base '{kb_slug}' not found."
        rows = await scoped_query(
            user_id,
            "SELECT body FROM kb_directives "
            "WHERE kb_id = $1 AND kind = 'guideline' AND is_active "
            "ORDER BY position, created_at",
            kb["id"],
        )
        if not rows:
            return f"No active guidelines for '{kb_slug}'."
        return "\n".join(f"- {r['body']}" for r in rows)
