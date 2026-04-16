"""Unit tests for guidelines-related pure functions.

Covers:
  - build_compile_prompt: guidelines_block and comments_by_page injection
  - build_streamlining_prompt: guidelines_block injection
  - render_guidelines_block: DB-backed helper (tested via pool fixture in integration suite;
    here we verify the rendered format from the service directly with a mock pool)
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sys
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))

from services.periodic_compile import PendingSource, build_compile_prompt  # noqa: E402
from services.wiki_streamlining import (  # noqa: E402
    StreamliningScope,
    StreamliningTarget,
    build_streamlining_prompt,
)
from services.kb_guidelines import render_guidelines_block  # noqa: E402


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _source(filename: str = "doc.md", path: str = "/", version: int = 1) -> PendingSource:
    return PendingSource(
        id="aaa",
        path=path,
        filename=filename,
        title="Doc",
        version=version,
        content_chars=100,
        updated_at=datetime.now(UTC),
    )


def _target(knowledge_base: str = "my-kb", prompt: str = "") -> StreamliningTarget:
    return StreamliningTarget(
        knowledge_base=knowledge_base,
        knowledge_base_id="kb-id",
        provider_api_key="key",
        provider="openrouter",
        model="claude-3",
        prompt=prompt,
        actor_user_id="user-id",
        interval_minutes=60,
        active_release_id="release-id",
    )


def _scope(
    scope_type: str = "targeted",
    pages: list | None = None,
    dirty_paths: list | None = None,
) -> StreamliningScope:
    return StreamliningScope(
        scope_type=scope_type,
        pages=pages
        or [
            {
                "page_key": "pk-1",
                "full_path": "/wiki/concepts/ai.md",
                "title": "AI",
                "tags": [],
                "content": "# AI",
            }
        ],
        dirty_paths=dirty_paths or [],
    )


# ─── build_compile_prompt ────────────────────────────────────────────────────


class TestBuildCompilePrompt:
    def test_source_listing_included(self):
        prompt = build_compile_prompt("my-kb", [_source()])
        assert "Changed sources:" in prompt
        assert "`/doc.md` (version 1)" in prompt

    def test_kb_name_in_prompt(self):
        prompt = build_compile_prompt("acme-kb", [_source()])
        assert "acme-kb" in prompt

    def test_guidelines_block_injected_before_sources(self):
        block = "<kb_guidelines>\n- Cite sources\n</kb_guidelines>"
        prompt = build_compile_prompt("kb", [_source()], guidelines_block=block)
        assert block in prompt
        assert prompt.index(block) < prompt.index("Changed sources:")

    def test_no_guidelines_by_default(self):
        prompt = build_compile_prompt("kb", [_source()])
        assert "<kb_guidelines>" not in prompt

    def test_comments_by_page_injected(self):
        comments = {"page-uuid-123": [{"id": "c1", "body": "Fix the intro"}]}
        prompt = build_compile_prompt("kb", [_source()], comments_by_page=comments)
        assert "editor_feedback" in prompt
        assert "Fix the intro" in prompt

    def test_empty_comments_dict_produces_no_feedback_section(self):
        prompt = build_compile_prompt("kb", [_source()], comments_by_page={})
        assert "editor_feedback" not in prompt

    def test_none_comments_produces_no_feedback_section(self):
        prompt = build_compile_prompt("kb", [_source()], comments_by_page=None)
        assert "editor_feedback" not in prompt

    def test_extra_prompt_appended_at_end(self):
        extra = "Focus on entity pages only."
        prompt = build_compile_prompt("kb", [_source()], extra_prompt=extra)
        assert "Additional instructions:" in prompt
        assert extra in prompt
        assert prompt.index("Additional instructions:") < prompt.index(extra)

    def test_guidelines_and_comments_both_present_in_correct_order(self):
        block = "<kb_guidelines>\n- Rule 1\n</kb_guidelines>"
        comments = {"key-abc": [{"id": "c2", "body": "needs diagram"}]}
        prompt = build_compile_prompt(
            "kb",
            [_source()],
            guidelines_block=block,
            comments_by_page=comments,
        )
        assert block in prompt
        assert "needs diagram" in prompt
        assert prompt.index(block) < prompt.index("needs diagram")

    def test_multiple_sources_all_listed(self):
        sources = [
            _source("a.md", "/", 1),
            _source("b.md", "/sub/", 2),
        ]
        prompt = build_compile_prompt("kb", sources)
        assert "`/a.md` (version 1)" in prompt
        assert "`/sub/b.md` (version 2)" in prompt


# ─── build_streamlining_prompt ───────────────────────────────────────────────


class TestBuildStreamliningPrompt:
    def test_kb_name_in_prompt(self):
        prompt = build_streamlining_prompt(_target("acme-kb"), _scope())
        assert "acme-kb" in prompt

    def test_scope_type_in_prompt(self):
        prompt = build_streamlining_prompt(_target(), _scope("full"))
        assert "full" in prompt

    def test_page_payload_in_prompt(self):
        prompt = build_streamlining_prompt(_target(), _scope())
        assert "/wiki/concepts/ai.md" in prompt

    def test_guidelines_block_injected_when_provided(self):
        block = "<kb_guidelines>\n- Use SVGs\n</kb_guidelines>"
        prompt = build_streamlining_prompt(_target(), _scope(), guidelines_block=block)
        assert block in prompt

    def test_no_guidelines_by_default(self):
        prompt = build_streamlining_prompt(_target(), _scope())
        assert "<kb_guidelines>" not in prompt

    def test_additional_instructions_when_prompt_set(self):
        t = _target(prompt="Enforce strict entity pages only.")
        prompt = build_streamlining_prompt(t, _scope())
        assert "Enforce strict entity pages only." in prompt

    def test_no_additional_instructions_when_prompt_empty(self):
        prompt = build_streamlining_prompt(_target(prompt=""), _scope())
        assert "Additional instructions:" not in prompt

    def test_dirty_paths_listed(self):
        scope = _scope(dirty_paths=["/wiki/concepts/old.md"])
        prompt = build_streamlining_prompt(_target(), scope)
        assert "/wiki/concepts/old.md" in prompt


# ─── render_guidelines_block ─────────────────────────────────────────────────


class TestRenderGuidelinesBlock:
    @pytest.mark.asyncio
    async def test_returns_empty_string_when_no_guidelines(self):
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=[])
        result = await render_guidelines_block(pool, "kb-id")
        assert result == ""

    @pytest.mark.asyncio
    async def test_wraps_bullets_in_kb_guidelines_tag(self):
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=[{"body": "Cite sources"}, {"body": "Use SVGs"}])
        result = await render_guidelines_block(pool, "kb-id")
        assert result.startswith("<kb_guidelines>")
        assert result.endswith("</kb_guidelines>")
        assert "- Cite sources" in result
        assert "- Use SVGs" in result

    @pytest.mark.asyncio
    async def test_single_guideline_formatted(self):
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=[{"body": "Only one rule"}])
        result = await render_guidelines_block(pool, "kb-id")
        assert result == "<kb_guidelines>\n- Only one rule\n</kb_guidelines>"
