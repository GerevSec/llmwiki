"""Supavault MCP tools — search, read, write."""

import re
import logging
from datetime import date
from fnmatch import fnmatch
from typing import Literal

from mcp.server.fastmcp import FastMCP, Context

from config import settings
from db import scoped_query, scoped_queryrow, scoped_execute

logger = logging.getLogger(__name__)

CONTEXT_CHARS = 120
MAX_LIST = 50
MAX_SEARCH = 20


def _user_id(ctx: Context) -> str:
    return ctx.request_context.access_token.client_id


def _deep_link(kb_slug: str, path: str, filename: str) -> str:
    full = (path.rstrip("/") + "/" + filename).lstrip("/")
    return f"{settings.APP_URL}/kb/{kb_slug}/{full}"


def _extract_snippet(content: str, pattern: str, is_regex: bool) -> str:
    if not content:
        return "(empty)"
    if is_regex:
        try:
            match = re.search(pattern, content, re.IGNORECASE)
        except re.error:
            match = None
    else:
        idx = content.lower().find(pattern.lower())
        if idx >= 0:
            match = type("M", (), {"start": lambda s: idx, "end": lambda s: idx + len(pattern)})()
        else:
            match = None

    if not match:
        return content[:CONTEXT_CHARS * 2].strip()

    start = max(0, match.start() - CONTEXT_CHARS)
    end = min(len(content), match.end() + CONTEXT_CHARS)
    snippet = content[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet = snippet + "..."
    return snippet


def _glob_match(filepath: str, pattern: str) -> bool:
    return fnmatch(filepath, pattern)


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="search",
        description=(
            "Browse or search the knowledge vault.\n\n"
            "Modes:\n"
            "- list: browse files and folders (query ignored)\n"
            "- exact: case-insensitive keyword search in note content\n"
            "- regex: regex search in note content\n\n"
            "If no knowledge_base is specified, lists all knowledge bases.\n"
            "Use glob patterns in `target` to scope (e.g. `reports/**`, `*.md`).\n"
            "Use `tags` to filter by document tags."
        ),
    )
    async def search(
        ctx: Context,
        mode: Literal["list", "exact", "regex"] = "list",
        knowledge_base: str = "",
        query: str = "",
        target: str = "*",
        tags: list[str] | None = None,
        limit: int = 10,
    ) -> str:
        user_id = _user_id(ctx)

        if not knowledge_base:
            return await _list_all_kbs(user_id)

        kb = await _resolve_kb(user_id, knowledge_base)
        if not kb:
            return f"Knowledge base '{knowledge_base}' not found."

        if mode == "list":
            return await _list_documents(user_id, kb, target, tags)
        elif mode in ("exact", "regex"):
            if not query:
                return f"{mode} mode requires a query."
            return await _search_content(user_id, kb, query, mode == "regex", target, tags, limit)

        return f"Unknown mode: {mode}"

    @mcp.tool(
        name="read",
        description=(
            "Read the full content of a note.\n\n"
            "Specify the knowledge base (slug) and the file path.\n"
            "For markdown notes, optionally extract specific sections by heading.\n"
            "Returns the note content, metadata, and a link to view it in the browser."
        ),
    )
    async def read(
        ctx: Context,
        knowledge_base: str,
        path: str,
        sections: list[str] | None = None,
    ) -> str:
        user_id = _user_id(ctx)

        kb = await _resolve_kb(user_id, knowledge_base)
        if not kb:
            return f"Knowledge base '{knowledge_base}' not found."

        path_clean = path.lstrip("/")
        if "/" in path_clean:
            dir_path = "/" + path_clean.rsplit("/", 1)[0] + "/"
            filename = path_clean.rsplit("/", 1)[1]
        else:
            dir_path = "/"
            filename = path_clean

        doc = await scoped_queryrow(
            user_id,
            "SELECT id, filename, title, path, content, tags, version, file_type, created_at, updated_at "
            "FROM documents WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
            kb["id"], filename, dir_path,
        )
        if not doc:
            doc = await scoped_queryrow(
                user_id,
                "SELECT id, filename, title, path, content, tags, version, file_type, created_at, updated_at "
                "FROM documents WHERE knowledge_base_id = $1 AND (filename = $2 OR title = $2) AND NOT archived",
                kb["id"], path_clean.split("/")[-1] if "/" in path_clean else path_clean,
            )

        if not doc:
            return f"Document '{path}' not found in {knowledge_base}."

        content = doc["content"] or ""

        if sections:
            content = _extract_sections(content, sections)

        tags_str = ", ".join(doc["tags"]) if doc["tags"] else "none"
        link = _deep_link(kb["slug"], doc["path"], doc["filename"])

        header = (
            f"**{doc['title'] or doc['filename']}**\n"
            f"Tags: {tags_str} | Version: {doc['version']} | "
            f"Updated: {doc['updated_at'].strftime('%Y-%m-%d') if doc['updated_at'] else 'unknown'}\n"
            f"[View in Supavault]({link})\n\n---\n\n"
        )
        return header + content

    @mcp.tool(
        name="write",
        description=(
            "Create or edit notes in the knowledge vault.\n\n"
            "Commands:\n"
            "- create: create a new note (title and tags are REQUIRED)\n"
            "- str_replace: replace exact text in an existing note (read first)\n"
            "- append: add content to the end of an existing note\n\n"
            "Returns a link to view the note in the browser."
        ),
    )
    async def write(
        ctx: Context,
        knowledge_base: str,
        command: Literal["create", "str_replace", "append"],
        path: str = "/",
        title: str = "",
        content: str = "",
        tags: list[str] | None = None,
        date_str: str = "",
        old_text: str = "",
        new_text: str = "",
    ) -> str:
        user_id = _user_id(ctx)

        kb = await _resolve_kb(user_id, knowledge_base)
        if not kb:
            return f"Knowledge base '{knowledge_base}' not found."

        if command == "create":
            return await _create_note(user_id, kb, path, title, content, tags or [], date_str)
        elif command == "str_replace":
            return await _edit_note(user_id, kb, path, old_text, new_text)
        elif command == "append":
            return await _append_note(user_id, kb, path, content)

        return f"Unknown command: {command}"

    # ── Search helpers ──

    async def _list_all_kbs(user_id: str) -> str:
        kbs = await scoped_query(
            user_id,
            "SELECT name, slug, created_at FROM knowledge_bases ORDER BY created_at DESC",
        )
        if not kbs:
            return "No knowledge bases found. Create one first."

        lines = ["**Knowledge Bases:**\n"]
        for kb in kbs:
            doc_count = await scoped_queryrow(
                user_id,
                "SELECT count(*) as cnt FROM documents WHERE knowledge_base_id = ("
                "SELECT id FROM knowledge_bases WHERE slug = $1) AND NOT archived",
                kb["slug"],
            )
            cnt = doc_count["cnt"] if doc_count else 0
            lines.append(f"  {kb['slug']}/ — {kb['name']} ({cnt} documents)")
        return "\n".join(lines)

    async def _resolve_kb(user_id: str, slug: str) -> dict | None:
        return await scoped_queryrow(
            user_id,
            "SELECT id, name, slug FROM knowledge_bases WHERE slug = $1",
            slug,
        )

    async def _list_documents(user_id: str, kb: dict, target: str, tags: list[str] | None) -> str:
        is_recursive = "**" in target

        if is_recursive:
            docs = await scoped_query(
                user_id,
                "SELECT id, filename, title, path, file_type, tags, updated_at "
                "FROM documents WHERE knowledge_base_id = $1 AND NOT archived "
                "ORDER BY path, filename",
                kb["id"],
            )
            if target not in ("**", "**/*"):
                glob_pat = "/" + target.lstrip("/")
                docs = [d for d in docs if _glob_match(d["path"] + d["filename"], glob_pat)]
        else:
            if "/" in target and not target.endswith("*"):
                parts = target.rsplit("/", 1)
                dir_path = "/" + parts[0].strip("/") + "/"
                file_pattern = parts[1] or "*"
            else:
                dir_path = "/"
                file_pattern = target

            docs = await scoped_query(
                user_id,
                "SELECT id, filename, title, path, file_type, tags, updated_at "
                "FROM documents WHERE knowledge_base_id = $1 AND path = $2 AND NOT archived "
                "ORDER BY filename",
                kb["id"], dir_path,
            )
            if file_pattern != "*":
                docs = [d for d in docs if _glob_match(d["filename"], file_pattern)]

        if tags:
            tag_set = {t.lower() for t in tags}
            docs = [d for d in docs if tag_set.issubset({t.lower() for t in (d["tags"] or [])})]

        subdirs = set()
        if not is_recursive:
            all_paths = await scoped_query(
                user_id,
                "SELECT DISTINCT path FROM documents WHERE knowledge_base_id = $1 AND NOT archived",
                kb["id"],
            )
            dir_path_check = dir_path if "/" in target and not target.endswith("*") else "/"
            for row in all_paths:
                p = row["path"]
                if p != dir_path_check and p.startswith(dir_path_check):
                    rest = p[len(dir_path_check):]
                    child = rest.split("/")[0]
                    if child:
                        subdirs.add(child)

        if not docs and not subdirs:
            return f"No matches for `{target}` in {kb['slug']}."

        lines = [f"**{kb['name']}** (`{target}`):\n"]
        for s in sorted(subdirs):
            lines.append(f"  {s}/")

        total = len(docs)
        for doc in docs[:MAX_LIST]:
            title = doc["title"] or doc["filename"]
            tag_str = f" [{', '.join(doc['tags'])}]" if doc["tags"] else ""
            date_part = f", {doc['updated_at'].strftime('%Y-%m-%d')}" if doc["updated_at"] else ""
            lines.append(f"  {doc['path']}{doc['filename']} ({doc['file_type']}{date_part}){tag_str}")

        if total > MAX_LIST:
            lines.append(f"  ... {total - MAX_LIST} more (narrow your search)")

        return "\n".join(lines)

    async def _search_content(
        user_id: str, kb: dict, query: str, is_regex: bool,
        target: str, tags: list[str] | None, limit: int,
    ) -> str:
        limit = min(limit, MAX_SEARCH)

        if is_regex:
            matches = await scoped_query(
                user_id,
                "SELECT id, filename, title, path, content, file_type, tags "
                "FROM documents WHERE knowledge_base_id = $1 AND NOT archived "
                "AND content ~* $2 LIMIT $3",
                kb["id"], query, limit,
            )
        else:
            matches = await scoped_query(
                user_id,
                "SELECT id, filename, title, path, content, file_type, tags "
                "FROM documents WHERE knowledge_base_id = $1 AND NOT archived "
                "AND content ILIKE $2 LIMIT $3",
                kb["id"], f"%{query}%", limit,
            )

        if target not in ("*", "**", "**/*"):
            glob_pat = "/" + target.lstrip("/")
            matches = [m for m in matches if _glob_match(m["path"] + m["filename"], glob_pat)]

        if tags:
            tag_set = {t.lower() for t in tags}
            matches = [m for m in matches if tag_set.issubset({t.lower() for t in (m["tags"] or [])})]

        if not matches:
            return f"No matches for `{query}` in {kb['slug']}."

        mode_label = "regex" if is_regex else "exact"
        lines = [f"**{len(matches)} match(es)** for `{query}` ({mode_label}):\n"]
        for m in matches:
            snippet = _extract_snippet(m["content"] or "", query, is_regex)
            link = _deep_link(kb["slug"], m["path"], m["filename"])
            lines.append(f"**{m['path']}{m['filename']}** — [view]({link})")
            lines.append(f"```\n{snippet}\n```\n")

        return "\n".join(lines)

    # ── Write helpers ──

    async def _create_note(
        user_id: str, kb: dict, path: str, title: str, content: str,
        tags: list[str], date_str: str,
    ) -> str:
        if not title:
            return "Error: title is required when creating a note."
        if not tags:
            return "Error: at least one tag is required when creating a note."

        dir_path = path if path.endswith("/") else path + "/"
        if not dir_path.startswith("/"):
            dir_path = "/" + dir_path

        filename = re.sub(r"[^\w\s\-.]", "", title.lower().replace(" ", "-"))
        if not filename.endswith(".md"):
            filename += ".md"

        note_date = date_str or date.today().isoformat()

        doc = await scoped_queryrow(
            user_id,
            "INSERT INTO documents (knowledge_base_id, user_id, filename, title, path, "
            "file_type, status, content, tags, version) "
            "VALUES ($1, auth.uid(), $2, $3, $4, 'md', 'ready', $5, $6, 0) "
            "RETURNING id, filename, path",
            kb["id"], filename, title, dir_path, content, tags,
        )

        link = _deep_link(kb["slug"], doc["path"], doc["filename"])
        return f"Created **{title}** at `{dir_path}{filename}`\nTags: {', '.join(tags)} | Date: {note_date}\n[View in Supavault]({link})"

    async def _edit_note(user_id: str, kb: dict, path: str, old_text: str, new_text: str) -> str:
        if not old_text:
            return "Error: old_text is required for str_replace."

        path_clean = path.lstrip("/")
        if "/" in path_clean:
            dir_path = "/" + path_clean.rsplit("/", 1)[0] + "/"
            filename = path_clean.rsplit("/", 1)[1]
        else:
            dir_path = "/"
            filename = path_clean

        doc = await scoped_queryrow(
            user_id,
            "SELECT id, content FROM documents "
            "WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
            kb["id"], filename, dir_path,
        )
        if not doc:
            return f"Document '{path}' not found."

        content = doc["content"] or ""
        count = content.count(old_text)
        if count == 0:
            return "Error: no match found for old_text."
        if count > 1:
            return f"Error: found {count} matches for old_text. Provide more context to match exactly once."

        new_content = content.replace(old_text, new_text, 1)
        await scoped_execute(
            user_id,
            "UPDATE documents SET content = $1, version = version + 1 WHERE id = $2",
            new_content, doc["id"],
        )

        link = _deep_link(kb["slug"], dir_path, filename)
        return f"Edited `{path}`. Replaced 1 occurrence.\n[View in Supavault]({link})"

    async def _append_note(user_id: str, kb: dict, path: str, content: str) -> str:
        path_clean = path.lstrip("/")
        if "/" in path_clean:
            dir_path = "/" + path_clean.rsplit("/", 1)[0] + "/"
            filename = path_clean.rsplit("/", 1)[1]
        else:
            dir_path = "/"
            filename = path_clean

        doc = await scoped_queryrow(
            user_id,
            "SELECT id, content FROM documents "
            "WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
            kb["id"], filename, dir_path,
        )
        if not doc:
            return f"Document '{path}' not found."

        new_content = (doc["content"] or "") + "\n\n" + content
        await scoped_execute(
            user_id,
            "UPDATE documents SET content = $1, version = version + 1 WHERE id = $2",
            new_content, doc["id"],
        )

        link = _deep_link(kb["slug"], dir_path, filename)
        return f"Appended to `{path}`.\n[View in Supavault]({link})"


def _extract_sections(content: str, section_names: list[str]) -> str:
    lines = content.split("\n")
    sections = []
    current_section = None
    current_lines = []

    for line in lines:
        if line.startswith("#"):
            if current_section and current_lines:
                sections.append((current_section, "\n".join(current_lines)))
            heading = line.lstrip("#").strip()
            current_section = heading
            current_lines = [line]
        elif current_section:
            current_lines.append(line)

    if current_section and current_lines:
        sections.append((current_section, "\n".join(current_lines)))

    wanted = {s.lower() for s in section_names}
    matched = [text for name, text in sections if name.lower() in wanted]

    if not matched:
        return f"No sections matching {section_names} found."
    return "\n\n".join(matched)
