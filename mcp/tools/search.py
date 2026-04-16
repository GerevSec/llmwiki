import logging
from fnmatch import fnmatch
from typing import Literal

from mcp.server.fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations

from db import scoped_query
from .helpers import get_user_id, resolve_kb, deep_link, MAX_LIST, MAX_SEARCH

logger = logging.getLogger(__name__)

CONTEXT_CHARS = 120


def _extract_snippet(content: str, query: str) -> str:
    if not content:
        return "(empty)"
    idx = content.lower().find(query.lower())
    if idx < 0:
        return content[:CONTEXT_CHARS * 2].strip()
    start = max(0, idx - CONTEXT_CHARS)
    end = min(len(content), idx + len(query) + CONTEXT_CHARS)
    snippet = content[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet = snippet + "..."
    return snippet


async def _list_documents(user_id: str, kb: dict, target: str, tags: list[str] | None) -> str:
    docs = await scoped_query(
        user_id,
        "SELECT id, filename, title, path, file_type, tags, page_count, updated_at "
        "FROM documents WHERE knowledge_base_id = $1 AND NOT archived "
        "ORDER BY path, filename",
        kb["id"],
    )

    if target not in ("*", "**", "**/*"):
        glob_pat = "/" + target.lstrip("/") if not target.startswith("/") else target
        docs = [d for d in docs if fnmatch(d["path"] + d["filename"], glob_pat)]

    if tags:
        tag_set = {t.lower() for t in tags}
        docs = [d for d in docs if tag_set.issubset({t.lower() for t in (d["tags"] or [])})]

    if not docs:
        return f"No matches for `{target}` in {kb['slug']}."

    sources = [d for d in docs if not d["path"].startswith("/wiki/")]
    wiki_pages = [d for d in docs if d["path"].startswith("/wiki/")]

    lines = [f"**{kb['name']}** (`{target}`):\n"]

    if sources:
        lines.append(f"**Sources ({len(sources)}):**")
        for doc in sources[:MAX_LIST]:
            tag_str = f" [{', '.join(doc['tags'])}]" if doc["tags"] else ""
            date_part = f", {doc['updated_at'].strftime('%Y-%m-%d')}" if doc["updated_at"] else ""
            pages_part = f", {doc['page_count']}p" if doc["page_count"] else ""
            lines.append(f"  {doc['path']}{doc['filename']} ({doc['file_type']}{pages_part}{date_part}){tag_str}")
        if len(sources) > MAX_LIST:
            lines.append(f"  ... {len(sources) - MAX_LIST} more")

    if wiki_pages:
        if sources:
            lines.append("")
        lines.append(f"**Wiki ({len(wiki_pages)} pages):**")
        for doc in wiki_pages[:MAX_LIST]:
            date_part = f", {doc['updated_at'].strftime('%Y-%m-%d')}" if doc["updated_at"] else ""
            lines.append(f"  {doc['path']}{doc['filename']}{date_part}")

    return "\n".join(lines)


async def _search_chunks(
    user_id: str, kb: dict, query: str, path: str,
    tags: list[str] | None, limit: int,
) -> str:
    path_filter = ""
    if path not in ("*", "**", "**/*"):
        if path.startswith("/wiki"):
            path_filter = " AND d.path LIKE '/wiki/%%'"
        elif path == "/" or path == "/*":
            path_filter = " AND d.path NOT LIKE '/wiki/%%'"

    matches = await scoped_query(
        user_id,
        f"SELECT dc.content, dc.page, dc.header_breadcrumb, dc.chunk_index, "
        f"  d.filename, d.title, d.path, d.file_type, d.tags, "
        f"  pgroonga_score(dc.tableoid, dc.ctid) AS score "
        f"FROM document_chunks dc "
        f"JOIN documents d ON dc.document_id = d.id "
        f"WHERE dc.knowledge_base_id = $1 "
        f"  AND dc.content &@~ $2 "
        f"  AND NOT d.archived"
        f"{path_filter} "
        f"ORDER BY score DESC, dc.chunk_index "
        f"LIMIT {limit}",
        kb["id"], query,
    )

    if tags:
        tag_set = {t.lower() for t in tags}
        matches = [m for m in matches if tag_set.issubset({t.lower() for t in (m.get("tags") or [])})]

    if not matches:
        return f"No matches for `{query}` in {kb['slug']}."

    lines = [f"**{len(matches)} result(s)** for `{query}`:\n"]
    for m in matches:
        filepath = f"{m['path']}{m['filename']}"
        page_str = f" (p.{m['page']})" if m['page'] else ""
        breadcrumb = f"\n  {m['header_breadcrumb']}" if m["header_breadcrumb"] else ""
        snippet = _extract_snippet(m["content"], query)
        link = deep_link(kb["slug"], m["path"], m["filename"])
        score = m.get("score", 0)
        score_str = f" [{score:.1f}]" if score else ""
        lines.append(f"**{filepath}**{page_str}{score_str} — [view]({link}){breadcrumb}")
        lines.append(f"```\n{snippet}\n```\n")

    return "\n".join(lines)


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="search",
        description=(
            "Browse or search a knowledge base.\n\n"
            "Sources (raw documents) live at `/`. Wiki pages (LLM-compiled) live at `/wiki/`.\n\n"
            "Modes:\n"
            "- list: browse files and folders\n"
            "- search: keyword search across document content (searches chunks for precise results with page numbers)\n\n"
            "Use `path` to scope: `*` for root, `/wiki/**` for wiki only, `*.pdf` for PDFs, etc.\n"
            "Use `tags` to filter by document tags.\n\n"
            "BREAKING CHANGE: `kb_slug` is now required. Call `list_knowledge_bases` first to find "
            "available slugs. Passing an empty string is no longer supported."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
    )
    async def search(
        ctx: Context,
        kb_slug: str,
        mode: Literal["list", "search"] = "list",
        query: str = "",
        path: str = "*",
        tags: list[str] | None = None,
        limit: int = 10,
    ) -> str:
        user_id = get_user_id(ctx)

        kb = await resolve_kb(user_id, kb_slug)
        if not kb:
            return f"Knowledge base '{kb_slug}' not found. Call `list_knowledge_bases` to see available KBs."

        if mode == "list":
            return await _list_documents(user_id, kb, path, tags)
        elif mode == "search":
            if not query:
                return "search mode requires a query."
            return await _search_chunks(user_id, kb, query, path, tags, min(limit, MAX_SEARCH))

        return f"Unknown mode: {mode}"
