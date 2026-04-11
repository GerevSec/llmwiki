from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from fnmatch import fnmatch
from typing import Any

from services.kb_access import require_kb_access

MAX_LIST = 50
MAX_SEARCH = 20
MAX_BATCH_CHARS = 120_000
PROTECTED_FILES = {("/wiki/", "overview.md"), ("/wiki/", "log.md")}


@dataclass(frozen=True)
class ToolContext:
    pool: Any
    user_id: str
    knowledge_base_slug: str


def tool_definitions_anthropic() -> list[dict[str, Any]]:
    return [
        {
            "name": "guide",
            "description": "Get KB guidance and confirm the wiki workflow for this knowledge base.",
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "search",
            "description": "Browse or search the knowledge base.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["list", "search"]},
                    "query": {"type": "string"},
                    "path": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "limit": {"type": "integer"},
                },
            },
        },
        {
            "name": "read",
            "description": "Read one document or a glob of documents in the knowledge base.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "pages": {"type": "string"},
                    "sections": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["path"],
            },
        },
        {
            "name": "write",
            "description": "Create, replace exact text in, or append to wiki pages/notes.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "enum": ["create", "str_replace", "append"]},
                    "path": {"type": "string"},
                    "title": {"type": "string"},
                    "content": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "date_str": {"type": "string"},
                    "old_text": {"type": "string"},
                    "new_text": {"type": "string"},
                },
                "required": ["command"],
            },
        },
        {
            "name": "delete",
            "description": "Archive a single file or glob match.",
            "input_schema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    ]


def tool_definitions_openrouter() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"],
            },
        }
        for tool in tool_definitions_anthropic()
    ]


async def execute_tool(context: ToolContext, name: str, arguments: dict[str, Any] | None = None) -> str:
    args = arguments or {}
    if name == "guide":
        return await tool_guide(context)
    if name == "search":
        return await tool_search(
            context,
            mode=args.get("mode", "list"),
            query=args.get("query", ""),
            path=args.get("path", "*"),
            tags=args.get("tags"),
            limit=int(args.get("limit", 10)),
        )
    if name == "read":
        return await tool_read(
            context,
            path=args.get("path", ""),
            pages=args.get("pages", ""),
            sections=args.get("sections"),
        )
    if name == "write":
        return await tool_write(
            context,
            command=args.get("command", "create"),
            path=args.get("path", "/"),
            title=args.get("title", ""),
            content=args.get("content", ""),
            tags=args.get("tags") or [],
            date_str=args.get("date_str", ""),
            old_text=args.get("old_text", ""),
            new_text=args.get("new_text", ""),
        )
    if name == "delete":
        return await tool_delete(context, path=args.get("path", ""))
    raise RuntimeError(f"Unknown tool: {name}")


async def _resolve_kb(context: ToolContext, roles=("owner", "admin", "editor", "viewer")) -> dict:
    access = await require_kb_access(context.pool, context.user_id, context.knowledge_base_slug, roles)
    return access.__dict__


async def tool_guide(context: ToolContext) -> str:
    kb = await _resolve_kb(context)
    counts = await context.pool.fetchrow(
        "SELECT "
        "  (SELECT count(*) FROM documents WHERE knowledge_base_id = $1 AND path NOT LIKE '/wiki/%%' AND NOT archived) AS source_count, "
        "  (SELECT count(*) FROM documents WHERE knowledge_base_id = $1 AND path LIKE '/wiki/%%' AND NOT archived) AS wiki_count",
        kb["id"],
    )
    return (
        f"Knowledge base: {kb['name']} (`{kb['slug']}`)\n"
        f"Sources: {counts['source_count']} | Wiki pages: {counts['wiki_count']}\n\n"
        "Workflow: read changed sources, update `/wiki/overview.md`, update/create relevant wiki pages, and append an ingest entry to `/wiki/log.md`."
    )


async def tool_search(
    context: ToolContext,
    mode: str = "list",
    query: str = "",
    path: str = "*",
    tags: list[str] | None = None,
    limit: int = 10,
) -> str:
    kb = await _resolve_kb(context)
    docs = [
        dict(row)
        for row in await context.pool.fetch(
            "SELECT id, filename, title, path, file_type, tags, page_count, updated_at, content "
            "FROM documents WHERE knowledge_base_id = $1 AND NOT archived ORDER BY path, filename",
            kb["id"],
        )
    ]
    if mode == "list":
        glob_pat = "/" + path.lstrip("/") if path not in ("*", "**", "**/*") and not path.startswith("/") else path
        if path not in ("*", "**", "**/*"):
            docs = [d for d in docs if fnmatch(d["path"] + d["filename"], glob_pat)]
        if tags:
            tag_set = {t.lower() for t in tags}
            docs = [d for d in docs if tag_set.issubset({t.lower() for t in (d["tags"] or [])})]
        if not docs:
            return f"No matches for `{path}`."
        return "\n".join(f"- {doc['path']}{doc['filename']}" for doc in docs[:MAX_LIST])

    if mode == "search":
        rows = await context.pool.fetch(
            "SELECT dc.content, dc.page, dc.header_breadcrumb, d.filename, d.path, d.tags "
            "FROM document_chunks dc "
            "JOIN documents d ON d.id = dc.document_id "
            "WHERE dc.knowledge_base_id = $1 AND dc.content &@~ $2 AND NOT d.archived "
            "LIMIT $3",
            kb["id"],
            query,
            min(limit, MAX_SEARCH),
        )
        if not rows:
            return f"No matches for `{query}`."
        return "\n\n".join(
            f"{row['path']}{row['filename']}" + (f" (p.{row['page']})" if row["page"] else "") + f"\n{row['content'][:300]}"
            for row in rows
        )
    return f"Unknown mode: {mode}"


async def tool_read(
    context: ToolContext,
    path: str,
    pages: str = "",
    sections: list[str] | None = None,
) -> str:
    kb = await _resolve_kb(context)
    if "*" in path or "?" in path:
        docs = await context.pool.fetch(
            "SELECT filename, path, content, page_count FROM documents "
            "WHERE knowledge_base_id = $1 AND NOT archived ORDER BY path, filename",
            kb["id"],
        )
        glob_pat = "/" + path.lstrip("/") if not path.startswith("/") else path
        matched = [dict(doc) for doc in docs if fnmatch(doc["path"] + doc["filename"], glob_pat)]
        if not matched:
            return f"No documents matching `{path}`."
        parts = []
        chars = 0
        for doc in matched:
            content = (doc.get("content") or "")[:5000]
            chars += len(content)
            if chars > MAX_BATCH_CHARS:
                break
            parts.append(f"## {doc['path']}{doc['filename']}\n\n{content}")
        return "\n\n".join(parts)

    clean_path = path.lstrip("/")
    if "/" in clean_path:
        dir_path = "/" + clean_path.rsplit("/", 1)[0] + "/"
        filename = clean_path.rsplit("/", 1)[1]
    else:
        dir_path = "/"
        filename = clean_path

    doc = await context.pool.fetchrow(
        "SELECT id, filename, title, path, content, file_type, page_count "
        "FROM documents WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
        kb["id"],
        filename,
        dir_path,
    )
    if not doc:
        return f"Document '{path}' not found."
    content = doc["content"] or ""
    if sections:
        content = _extract_sections(content, sections)
    return f"{doc['path']}{doc['filename']}\n\n{content}"


async def tool_write(
    context: ToolContext,
    command: str,
    path: str = "/",
    title: str = "",
    content: str = "",
    tags: list[str] | None = None,
    date_str: str = "",
    old_text: str = "",
    new_text: str = "",
) -> str:
    kb = await _resolve_kb(context, ("owner", "admin", "editor"))
    tags = tags or []
    if command == "create":
        if not title:
            return "Error: title is required."
        dir_path = path if path.endswith("/") else path + "/"
        if not dir_path.startswith("/"):
            dir_path = "/" + dir_path
        filename = re.sub(r"[^\w\s\-.]", "", title.lower().replace(" ", "-"))
        if not filename.endswith(".md"):
            filename += ".md"
        row = await context.pool.fetchrow(
            "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, file_type, status, content, tags, version) "
            "VALUES ($1, $2, $3, $4, $5, 'md', 'ready', $6, $7, 0) RETURNING path, filename",
            kb["id"],
            context.user_id,
            filename,
            title,
            dir_path,
            content,
            tags,
        )
        return f"Created `{row['path']}{row['filename']}`"

    clean_path = path.lstrip("/")
    dir_path = "/" + clean_path.rsplit("/", 1)[0] + "/" if "/" in clean_path else "/"
    filename = clean_path.rsplit("/", 1)[1] if "/" in clean_path else clean_path
    doc = await context.pool.fetchrow(
        "SELECT id, content FROM documents WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
        kb["id"],
        filename,
        dir_path,
    )
    if not doc:
        return f"Document '{path}' not found."

    if command == "str_replace":
        if old_text not in (doc["content"] or ""):
            return "Error: old_text not found."
        new_content = (doc["content"] or "").replace(old_text, new_text, 1)
    elif command == "append":
        new_content = (doc["content"] or "") + "\n\n" + content
    else:
        return f"Unknown command: {command}"

    await context.pool.execute(
        "UPDATE documents SET content = $1, version = version + 1, updated_at = now() WHERE id = $2",
        new_content,
        doc["id"],
    )
    return f"Updated `{path}`"


async def tool_delete(context: ToolContext, path: str) -> str:
    kb = await _resolve_kb(context, ("owner", "admin", "editor"))
    if not path:
        return "Error: path is required."
    docs = await context.pool.fetch(
        "SELECT id, path, filename FROM documents WHERE knowledge_base_id = $1 AND NOT archived ORDER BY path, filename",
        kb["id"],
    )
    if "*" in path or "?" in path:
        glob_pat = "/" + path.lstrip("/") if not path.startswith("/") else path
        matched = [dict(doc) for doc in docs if fnmatch(doc["path"] + doc["filename"], glob_pat)]
    else:
        matched = [dict(doc) for doc in docs if doc["path"] + doc["filename"] == ("/" + path.lstrip("/"))]

    deletable = [doc for doc in matched if (doc["path"], doc["filename"]) not in PROTECTED_FILES]
    if not deletable:
        return "No deletable documents matched."
    await context.pool.execute(
        "UPDATE documents SET archived = true, updated_at = now() WHERE id = ANY($1::uuid[])",
        [str(doc["id"]) for doc in deletable],
    )
    return "\n".join(f"Deleted {doc['path']}{doc['filename']}" for doc in deletable)


def _extract_sections(content: str, section_names: list[str]) -> str:
    lines = content.splitlines()
    sections = []
    current_name = None
    current_lines = []
    wanted = {name.lower() for name in section_names}
    for line in lines:
        if line.startswith("#"):
            if current_name and current_name.lower() in wanted:
                sections.append("\n".join(current_lines))
            current_name = line.lstrip("#").strip()
            current_lines = [line]
        elif current_name:
            current_lines.append(line)
    if current_name and current_name.lower() in wanted:
        sections.append("\n".join(current_lines))
    return "\n\n".join(sections) if sections else content
