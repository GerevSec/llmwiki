"""Unit tests for merge_release_pages coherence + path-normalization guards.

These validate the fixes for the user-reported wiki bugs:
- Overview page got "Merged from X" literal + duplicated content after streamline
- tool_write + streamlining apply allowed /wiki/*.md/ nested paths
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))


# ─── Fakes ────────────────────────────────────────────────────────────────


@dataclass
class FakePage:
    page_key: str
    path: str
    filename: str
    title: str
    content: str
    tags: list[str]
    sort_order: int = 0

    @property
    def full_path(self) -> str:
        return f"{self.path}{self.filename}".replace("//", "/")


class FakeReleaseStore:
    """In-memory substitute for the wiki_release_pages subset we need for merge tests."""

    def __init__(self, pages: list[FakePage]):
        self._pages: dict[str, FakePage] = {p.page_key: p for p in pages}

    def get(self, page_key: str) -> FakePage | None:
        return self._pages.get(page_key)

    def list(self) -> list[FakePage]:
        return list(self._pages.values())

    def upsert(self, *, page_key: str, content: str, path: str, filename: str, title: str, tags: list[str], sort_order: int = 0) -> FakePage:
        page = FakePage(page_key=page_key, path=path, filename=filename, title=title, content=content, tags=tags, sort_order=sort_order)
        self._pages[page_key] = page
        return page

    def delete(self, page_key: str) -> None:
        self._pages.pop(page_key, None)


# ─── US-102: merge coherence ──────────────────────────────────────────────


class TestMergeReleasePagesCoherence:
    """merge_release_pages must produce coherent content without literal
    'Merged from X' headers, without duplicating paragraphs that already
    exist in the target. The resulting page should read as if it were
    written that way from the start."""

    def test_merge_does_not_emit_merged_from_header(self):
        from services.wiki_releases import _merge_content_coherently

        target_content = "# Overview\n\nAlpha is a project.\n\nBeta is its sibling.\n"
        source_content = "# Alpha Detail\n\nAlpha is a project.\n\nAlpha has a logo.\n"

        merged = _merge_content_coherently(
            target_content=target_content,
            target_title="Overview",
            source_content=source_content,
            source_title="Alpha Detail",
        )

        assert "Merged from" not in merged, f"merge output still contains 'Merged from': {merged}"
        assert "## Alpha Detail" not in merged or "## Overview" in merged, (
            f"merged content should not inject the source title as a sub-heading: {merged}"
        )

    def test_merge_dedupes_blocks_already_in_target(self):
        from services.wiki_releases import _merge_content_coherently

        target_content = "# Overview\n\nAlpha is a project.\n\nBeta is its sibling.\n"
        source_content = "# Source\n\nAlpha is a project.\n\nAlpha has a logo.\n"

        merged = _merge_content_coherently(
            target_content=target_content,
            target_title="Overview",
            source_content=source_content,
            source_title="Source",
        )

        # The "Alpha is a project." block appears in both — should appear
        # exactly once in the merged output, not twice.
        occurrences = merged.count("Alpha is a project.")
        assert occurrences == 1, f"dedupe failed: 'Alpha is a project.' appears {occurrences}× in merged output"

    def test_merge_preserves_source_only_facts(self):
        from services.wiki_releases import _merge_content_coherently

        target_content = "# Overview\n\nAlpha is a project.\n"
        source_content = "# Source\n\nAlpha is a project.\n\nAlpha has a logo.\n"

        merged = _merge_content_coherently(
            target_content=target_content,
            target_title="Overview",
            source_content=source_content,
            source_title="Source",
        )

        assert "logo" in merged, "merge dropped source-only fact 'Alpha has a logo.'"

    def test_merge_is_idempotent_on_fully_subsumed_source(self):
        from services.wiki_releases import _merge_content_coherently

        target_content = "# Overview\n\nAlpha is a project.\n\nBeta is its sibling.\n"
        source_content = "# Source\n\nAlpha is a project.\n"

        merged = _merge_content_coherently(
            target_content=target_content,
            target_title="Overview",
            source_content=source_content,
            source_title="Source",
        )

        # Source adds nothing new, merged should equal target (no 'Merged from' noise)
        assert merged.strip() == target_content.strip()


# ─── US-104: path normalization guards ────────────────────────────────────


class TestStripBrokenReleaseLinks:
    """Regression: publish_release used to fail the whole run when validate_release
    found any broken internal wiki links. strip_broken_release_links now reduces
    `[text](/wiki/broken.md)` to plain `text` before validation so a handful of
    stale cross-references can't nuke 17 pages of otherwise-good content."""

    def test_broken_wiki_link_is_stripped_to_plain_text(self):
        import re

        from services.wiki_releases import _WIKI_MD_LINK_RE

        content = "See [Life Agent](/wiki/concepts/life-agent.md) and [Missing Page](/wiki/concepts/missing.md)."
        known_paths = {"/wiki/concepts/life-agent.md"}

        def replace(match: re.Match[str]) -> str:
            text = match.group(1)
            href = match.group(2)
            return match.group(0) if href in known_paths else text

        stripped = _WIKI_MD_LINK_RE.sub(replace, content)
        assert "(/wiki/concepts/missing.md)" not in stripped
        assert "(/wiki/concepts/life-agent.md)" in stripped
        assert "Missing Page" in stripped
        assert "Life Agent" in stripped

    def test_regex_only_matches_wiki_paths(self):
        from services.wiki_releases import _WIKI_MD_LINK_RE

        content = "[Google](https://google.com) and [Relative](./foo.md)"
        matches = _WIKI_MD_LINK_RE.findall(content)
        assert not matches, f"regex should not match external or relative links, got {matches}"


class TestFlatWikiPathNormalization:
    """tool_write create, streamlining apply, and rename ops must never leave
    pages at /wiki/<something>.md/<leaf>.md. Any such attempt is normalized to
    /wiki/<leaf>.md so the wiki stays flat."""

    def test_normalize_wiki_path_flattens_nested_md_parent(self):
        from services.wiki_releases import normalize_flat_wiki_path

        dir_path, filename = normalize_flat_wiki_path(
            "/wiki/architecture.md/",
            "architecture-overview.md",
        )
        assert dir_path == "/wiki/"
        assert filename == "architecture-overview.md"

    def test_normalize_wiki_path_passthrough_for_flat_layout(self):
        from services.wiki_releases import normalize_flat_wiki_path

        dir_path, filename = normalize_flat_wiki_path("/wiki/", "overview.md")
        assert dir_path == "/wiki/"
        assert filename == "overview.md"

    def test_normalize_wiki_path_flattens_deeper_md_parents(self):
        from services.wiki_releases import normalize_flat_wiki_path

        dir_path, filename = normalize_flat_wiki_path(
            "/wiki/foo.md/bar.md/",
            "baz.md",
        )
        # Any ancestor that ends in .md is a malformed nested path — collapse to /wiki/
        assert dir_path == "/wiki/"
        assert filename == "baz.md"

    def test_normalize_wiki_path_keeps_genuine_subdirectories(self):
        """Subdirectories without the .md suffix (e.g. /wiki/architecture/)
        are legitimate tree layout and must be preserved."""
        from services.wiki_releases import normalize_flat_wiki_path

        dir_path, filename = normalize_flat_wiki_path(
            "/wiki/architecture/",
            "overview.md",
        )
        assert dir_path == "/wiki/architecture/"
        assert filename == "overview.md"

    def test_tool_write_create_refuses_nested_md_parent(self):
        """End-to-end: passing path=/wiki/foo.md with title=Foo Bar into
        tool_write create must land the page at /wiki/foo.md, not
        /wiki/foo.md/foo-bar.md and not /wiki/foo.md/foo.md."""
        import asyncio

        from services.compile_tools import ToolContext, tool_write

        captured: dict = {}

        class StubPool:
            async def fetchrow(self, query, *args):
                if "knowledge_base_memberships" in query:
                    return {
                        "id": "kb-id",
                        "slug": "kb",
                        "name": "KB",
                        "role": "owner",
                        "owner_user_id": "user-id",
                    }
                if "INSERT INTO documents" in query:
                    # args: knowledge_base_id, user_id, filename, title, path, content, tags
                    captured["dir_path"] = args[4]
                    captured["filename"] = args[2]
                    return {"path": args[4], "filename": args[2]}
                return None

            async def fetch(self, *_, **__):
                return []

            async def execute(self, *_, **__):
                return "OK"

        ctx = ToolContext(
            pool=StubPool(),
            user_id="user-id",
            knowledge_base_slug="kb",
            wiki_release_id=None,
        )
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                tool_write(ctx, command="create", path="/wiki/foo.md", title="Foo Bar", content="x")
            )
        finally:
            loop.close()

        assert "/wiki/foo.md/foo-bar.md" not in result
        assert "/wiki/foo.md/foo.md" not in result
        assert "/wiki/foo.md" in result
