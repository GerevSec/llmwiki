"""One-off data cleanup for bes-wiki:

1. Flatten every nested /wiki/*.md/ path in the active release to /wiki/<leaf>.
2. Merge pages that collide at the same flat path via
   `_merge_content_coherently` so no content is lost.
3. Strip legacy '## Merged from X' artifacts from page bodies by re-running
   the merge function against target.content as both target and source (which
   is idempotent and dedupes residual duplication from old streamlines).

Usage (from repo root, with .venv active, DATABASE_URL exported):
    ./.venv/bin/python api/scripts/cleanup_bes_wiki_paths.py --apply

Without --apply the script prints the planned changes and exits.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))

from services.wiki_releases import (  # noqa: E402
    _merge_content_coherently,
    _normalize_coverage_unit,
    normalize_flat_wiki_path,
)

KB_ID = "9ebdf1c9-98f4-4de3-ae54-65da0b3e48ae"


@dataclass
class PageRow:
    page_key: str
    path: str
    filename: str
    title: str | None
    content: str
    tags: list[str]
    sort_order: int


def _strip_merged_from_artifacts(content: str) -> str:
    """Remove literal '## Merged from X' headers and dedupe repeated blocks
    inside a single page body — this cleans up pages that were mangled by
    the old merge concatenation bug."""
    if "Merged from" not in content:
        return content
    import re

    # Drop any "## Merged from ..." header line (content beneath it stays)
    cleaned = re.sub(r"^\s*##+\s*Merged from.*$", "", content, flags=re.MULTILINE)
    # Collapse triple-newlines back to double
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip() + "\n"

    # Dedupe blocks: keep the first occurrence of each normalized block.
    blocks = re.split(r"\n\s*\n", cleaned)
    seen: set[str] = set()
    kept: list[str] = []
    for block in blocks:
        stripped = block.strip()
        if not stripped:
            continue
        normalized = _normalize_coverage_unit(stripped)
        if normalized and normalized in seen:
            continue
        if normalized:
            seen.add(normalized)
        kept.append(stripped)
    return "\n\n".join(kept) + "\n"


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually update the DB. Without this flag, runs in dry-run mode.")
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL env var is not set", file=sys.stderr)
        return 2

    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            active_release_id = await conn.fetchval(
                "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
                KB_ID,
            )
            if not active_release_id:
                print("ERROR: active_wiki_release_id is null for bes-wiki", file=sys.stderr)
                return 3
            print(f"[+] active release: {active_release_id}")

            rows = await conn.fetch(
                "SELECT page_key::text, path, filename, title, content, tags, sort_order "
                "FROM wiki_release_pages WHERE release_id = $1::uuid ORDER BY path, filename",
                active_release_id,
            )
            pages = [
                PageRow(
                    page_key=row["page_key"],
                    path=row["path"],
                    filename=row["filename"],
                    title=row["title"],
                    content=row["content"] or "",
                    tags=list(row["tags"] or []),
                    sort_order=row["sort_order"] or 0,
                )
                for row in rows
            ]
            print(f"[+] {len(pages)} pages in active release")
            for page in pages:
                print(f"    {page.path}{page.filename}  ({len(page.content)} chars)")

            # 1. Compute target flat path + filename for every page.
            flat_paths: dict[str, tuple[str, str]] = {}
            for page in pages:
                new_path, new_filename = normalize_flat_wiki_path(page.path, page.filename)
                flat_paths[page.page_key] = (new_path, new_filename)

            # 2. Group by (new_path, new_filename) to find collisions.
            by_full: dict[tuple[str, str], list[PageRow]] = {}
            for page in pages:
                key = flat_paths[page.page_key]
                by_full.setdefault(key, []).append(page)

            # 2b. Additional consolidation: fold ingest-log.md into log.md so
            # the menu shows one canonical log page (matches user request).
            # This is applied BEFORE the collision loop by rewriting the flat
            # target for ingest-log.md → log.md when log.md is present.
            has_log = any(
                key == ("/wiki/", "log.md")
                for key in flat_paths.values()
            )
            if has_log:
                for page_key, (new_path, new_filename) in list(flat_paths.items()):
                    if new_path == "/wiki/" and new_filename == "ingest-log.md":
                        flat_paths[page_key] = ("/wiki/", "log.md")
                # Rebuild by_full after the rewrite.
                by_full = {}
                for page in pages:
                    key = flat_paths[page.page_key]
                    by_full.setdefault(key, []).append(page)

            plan: list[dict[str, Any]] = []
            for (new_path, new_filename), group in by_full.items():
                if len(group) == 1 and (group[0].path, group[0].filename) == (new_path, new_filename):
                    # Already flat with no duplicates — just strip merged-from artifacts.
                    original = group[0]
                    cleaned = _strip_merged_from_artifacts(original.content)
                    if cleaned != original.content:
                        plan.append({
                            "op": "rewrite-content",
                            "page_key": original.page_key,
                            "path": original.path,
                            "filename": original.filename,
                            "before_chars": len(original.content),
                            "after_chars": len(cleaned),
                            "new_content": cleaned,
                        })
                    continue

                # Pick canonical: largest content wins.
                canonical = max(group, key=lambda p: len(p.content))
                canonical_cleaned = _strip_merged_from_artifacts(canonical.content)
                merged_content = canonical_cleaned
                merged_tags: set[str] = set(canonical.tags)
                for other in group:
                    if other.page_key == canonical.page_key:
                        continue
                    source_cleaned = _strip_merged_from_artifacts(other.content)
                    merged_content = _merge_content_coherently(
                        target_content=merged_content,
                        target_title=canonical.title or canonical.filename,
                        source_content=source_cleaned,
                        source_title=other.title or other.filename,
                    )
                    merged_tags.update(other.tags)

                plan.append({
                    "op": "consolidate",
                    "keep_page_key": canonical.page_key,
                    "keep_old_path": canonical.path,
                    "keep_old_filename": canonical.filename,
                    "new_path": new_path,
                    "new_filename": new_filename,
                    "merged_content": merged_content,
                    "merged_tags": sorted(merged_tags),
                    "delete_page_keys": [p.page_key for p in group if p.page_key != canonical.page_key],
                    "delete_full_paths": [f"{p.path}{p.filename}" for p in group if p.page_key != canonical.page_key],
                    "before_chars": len(canonical.content),
                    "after_chars": len(merged_content),
                })

            print("\n[+] planned changes:")
            for item in plan:
                if item["op"] == "rewrite-content":
                    print(f"    rewrite {item['path']}{item['filename']}: {item['before_chars']} -> {item['after_chars']} chars")
                else:
                    print(f"    consolidate -> {item['new_path']}{item['new_filename']}: {item['before_chars']} -> {item['after_chars']} chars, delete {len(item['delete_page_keys'])} peer(s)")

            if not args.apply:
                print("\n[dry-run] not applied. Re-run with --apply to execute.")
                return 0

            # Apply changes in a single transaction.
            async with conn.transaction():
                for item in plan:
                    if item["op"] == "rewrite-content":
                        await conn.execute(
                            "UPDATE wiki_release_pages SET content = $1 WHERE release_id = $2::uuid AND page_key = $3::uuid",
                            item["new_content"],
                            active_release_id,
                            item["page_key"],
                        )
                    else:
                        await conn.execute(
                            "UPDATE wiki_release_pages SET path = $1, filename = $2, content = $3, tags = $4::text[] "
                            "WHERE release_id = $5::uuid AND page_key = $6::uuid",
                            item["new_path"],
                            item["new_filename"],
                            item["merged_content"],
                            item["merged_tags"],
                            active_release_id,
                            item["keep_page_key"],
                        )
                        if item["delete_page_keys"]:
                            await conn.execute(
                                "DELETE FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = ANY($2::uuid[])",
                                active_release_id,
                                item["delete_page_keys"],
                            )
            print("\n[+] applied. Fetching post-state for verification...")
            post = await conn.fetch(
                "SELECT path, filename, char_length(content) AS chars FROM wiki_release_pages "
                "WHERE release_id = $1::uuid ORDER BY path, filename",
                active_release_id,
            )
            for row in post:
                print(f"    {row['path']}{row['filename']}  ({row['chars']} chars)")
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
