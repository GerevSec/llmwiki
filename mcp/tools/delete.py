from mcp.server.fastmcp import FastMCP, Context

from db import scoped_query, scoped_queryrow, service_execute, get_pool
from .helpers import get_user_id, require_kb_role, glob_match, resolve_path
from .wiki_release import (
    create_draft_release,
    delete_release_page,
    get_release_pages,
    publish_release,
    prune_old_releases,
    record_dirty_scope,
)

_PROTECTED_FILES = {("/wiki/", "overview.md"), ("/wiki/", "log.md")}


def _is_protected(doc: dict) -> bool:
    return (doc["path"], doc["filename"]) in _PROTECTED_FILES


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="delete",
        description=(
            "Delete documents or wiki pages from the knowledge vault.\n\n"
            "Provide a path to delete a single file, or a glob pattern to delete multiple.\n"
            "Examples:\n"
            "- `path=\"old-notes.md\"` — delete a single file\n"
            "- `path=\"/wiki/drafts/*\"` — delete all files in a folder\n"
            "- `path=\"/wiki/**\"` — delete the entire wiki\n\n"
            "Note: overview.md and log.md are structural pages and cannot be deleted.\n"
            "Returns a list of deleted files. This action cannot be undone."
        ),
    )
    async def delete(
        ctx: Context,
        kb_slug: str,
        path: str,
    ) -> str:
        user_id = get_user_id(ctx)

        try:
            kb = await require_kb_role(user_id, kb_slug, "owner", "admin", "editor")
        except RuntimeError:
            return f"Knowledge base '{kb_slug}' not found."

        if not path or path in ("*", "**", "**/*"):
            return "Error: refusing to delete everything. Use a more specific path."

        is_glob = "*" in path or "?" in path

        if is_glob:
            docs = await scoped_query(
                user_id,
                "SELECT id, filename, title, path FROM documents "
                "WHERE knowledge_base_id = $1 AND NOT archived ORDER BY path, filename",
                kb["id"],
            )
            glob_pat = "/" + path.lstrip("/") if not path.startswith("/") else path
            matched = [d for d in docs if glob_match(d["path"] + d["filename"], glob_pat)]
        else:
            dir_path, filename = resolve_path(path)

            doc = await scoped_queryrow(
                user_id,
                "SELECT id, filename, title, path FROM documents "
                "WHERE knowledge_base_id = $1 AND filename = $2 AND path = $3 AND NOT archived",
                kb["id"], filename, dir_path,
            )
            matched = [doc] if doc else []

        if not matched:
            return f"No documents matching `{path}` found in {kb_slug}."

        protected = [d for d in matched if _is_protected(d)]
        deletable = [d for d in matched if not _is_protected(d)]

        if not deletable:
            names = ", ".join(f"`{d['path']}{d['filename']}`" for d in protected)
            return f"Cannot delete {names} — these are structural wiki pages. Use `write` to edit their content instead."

        if all(d["path"].startswith("/wiki/") for d in deletable):
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    draft_release_id, _ = await create_draft_release(conn, kb["id"], created_by="mcp")
                    draft_pages = await get_release_pages(conn, draft_release_id)
                    by_path = {page.path + page.filename: page for page in draft_pages}
                    deleted_paths: list[str] = []
                    for d in deletable:
                        page = by_path.get(d["path"] + d["filename"])
                        if not page:
                            continue
                        await delete_release_page(conn, draft_release_id, page.page_key)
                        deleted_paths.append(f"{page.path}{page.filename}")
                        await record_dirty_scope(conn, kb["id"], full_path=f"{page.path}{page.filename}", reason="mcp_delete")
                    await publish_release(conn, kb["id"], draft_release_id, actor_user_id=user_id, mode="mcp")
                    await prune_old_releases(conn, kb["id"])
            lines = [f"Deleted {len(deletable)} document(s):\n"]
            for d in deletable:
                lines.append(f"  {d['path']}{d['filename']}")
            if protected:
                names = ", ".join(f"`{d['path']}{d['filename']}`" for d in protected)
                lines.append(f"\nSkipped (protected): {names}")
            return "\n".join(lines)

        doc_ids = [str(d["id"]) for d in deletable]
        await service_execute(
            "UPDATE documents SET archived = true, updated_at = now() "
            "WHERE id = ANY($1::uuid[])",
            doc_ids,
        )

        lines = [f"Deleted {len(deletable)} document(s):\n"]
        for d in deletable:
            lines.append(f"  {d['path']}{d['filename']}")

        if protected:
            names = ", ".join(f"`{d['path']}{d['filename']}`" for d in protected)
            lines.append(f"\nSkipped (protected): {names}")

        return "\n".join(lines)
