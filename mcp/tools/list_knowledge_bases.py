from mcp.server.fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations

from db import scoped_query
from .helpers import get_user_id


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="list_knowledge_bases",
        description=(
            "List all knowledge bases accessible to the authenticated user.\n\n"
            "Call this first to discover available KBs and their slugs. "
            "Pass the returned `kb_slug` to `search`, `read`, `write`, `delete`, "
            "and `get_kb_guidelines`."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
    )
    async def list_knowledge_bases(ctx: Context) -> str:
        user_id = get_user_id(ctx)
        rows = await scoped_query(
            user_id,
            "SELECT kb.slug AS kb_slug, kb.id::text AS kb_id, kb.name, m.role "
            "FROM knowledge_bases kb "
            "JOIN knowledge_base_memberships m ON m.knowledge_base_id = kb.id AND m.user_id = $1 "
            "ORDER BY kb.name",
            user_id,
        )
        if not rows:
            return "No knowledge bases found."
        lines = ["**Your Knowledge Bases:**\n"]
        for r in rows:
            lines.append(f"- **{r['name']}** (`{r['kb_slug']}`) — {r['role']}")
        return "\n".join(lines)
