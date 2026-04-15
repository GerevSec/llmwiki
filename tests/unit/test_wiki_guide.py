"""Unit tests for the centralized WIKI_GUIDE_TEXT doctrine.

These validate that:
- api.services.wiki_guide and mcp.tools.guide share the SAME canonical text
- tool_guide returns the full doctrine, not a stub
- build_compile_prompt inlines the doctrine and does NOT contain the
  contradictory flat-domain bullets that were producing the flat wiki
- build_streamlining_prompt inlines the doctrine too

No real API keys or DB connections are required.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))

from services.wiki_guide import WIKI_GUIDE_TEXT  # noqa: E402


# ─── Doctrine integrity ───────────────────────────────────────────────────


class TestWikiGuideText:
    def test_has_core_section_headings(self):
        """The guide must include every load-bearing section the compile
        flow depends on. Changing these headings is a breaking change."""
        required_snippets = [
            "# LLM Wiki — How It Works",
            "## Architecture",
            "## Wiki Structure",
            "### Overview (`/wiki/overview.md`)",
            "### Concepts (`/wiki/concepts/`)",
            "### Entities (`/wiki/entities/`)",
            "### Log (`/wiki/log.md`)",
            "## Writing Standards",
            "### Visual Elements — MANDATORY",
            "### Citations — REQUIRED",
            "### Cross-References",
            "## Core Workflows",
        ]
        missing = [s for s in required_snippets if s not in WIKI_GUIDE_TEXT]
        assert not missing, f"WIKI_GUIDE_TEXT missing required sections: {missing}"

    def test_mandates_footnote_citations(self):
        assert "[^1]" in WIKI_GUIDE_TEXT, "guide must show footnote syntax"
        assert "[^2]" in WIKI_GUIDE_TEXT

    def test_mandates_mermaid_diagrams(self):
        assert "```mermaid" in WIKI_GUIDE_TEXT

    def test_structure_prescribes_concepts_and_entities_primary(self):
        assert "/wiki/concepts/" in WIKI_GUIDE_TEXT
        assert "/wiki/entities/" in WIKI_GUIDE_TEXT


class TestApiMcpGuideIdentical:
    def test_api_and_mcp_wiki_guide_text_are_identical(self):
        """Drift guard: the api and mcp copies of the guide text MUST stay
        in sync. This test imports both and asserts string equality. If it
        ever fails, update both copies to match (do not silently diverge)."""
        src_path = ROOT / "mcp" / "tools" / "guide.py"
        source = src_path.read_text()
        match = re.search(r'GUIDE_TEXT = """(.*?)"""', source, re.DOTALL)
        assert match is not None, "could not locate GUIDE_TEXT in mcp/tools/guide.py"
        mcp_guide_text = match.group(1)
        assert WIKI_GUIDE_TEXT == mcp_guide_text, (
            "WIKI_GUIDE_TEXT drifted from mcp/tools/guide.py GUIDE_TEXT. "
            f"api_len={len(WIKI_GUIDE_TEXT)}, mcp_len={len(mcp_guide_text)}. "
            "Update both copies to match."
        )


# ─── Compile prompt inlining ──────────────────────────────────────────────


class TestBuildCompilePromptInlines:
    def _sample_prompt(self) -> str:
        from services.periodic_compile import PendingSource, build_compile_prompt

        sources = [
            PendingSource(
                id="1",
                path="/",
                filename="notes.pdf",
                title="Notes",
                version=1,
                content_chars=100,
                updated_at=None,
            )
        ]
        return build_compile_prompt("example-kb", sources)

    def test_prompt_contains_full_guide_signature_substrings(self):
        prompt = self._sample_prompt()
        for must_have in [
            "/wiki/concepts/",
            "/wiki/entities/",
            "Citations — REQUIRED",
            "Visual Elements — MANDATORY",
        ]:
            assert must_have in prompt, f"compile prompt is missing {must_have!r}"

    def test_prompt_does_not_contain_contradictory_flat_guidance(self):
        """The previous flat-wiki bugs came from build_compile_prompt
        directly recommending 'one folder per domain such as
        /wiki/architecture/' and 'durable subject-matter categories'. The
        guide uses concepts/entities, so these must be gone."""
        prompt = self._sample_prompt()
        forbidden = [
            "one folder per domain",
            "architecture/business/team",
            "durable subject-matter categories",
            "/wiki/business/pricing.md",
        ]
        hits = [f for f in forbidden if f in prompt]
        assert not hits, f"compile prompt still contains contradictory guidance: {hits}"

    def test_prompt_still_lists_changed_sources(self):
        prompt = self._sample_prompt()
        assert "`/notes.pdf` (version 1)" in prompt

    def test_prompt_still_contains_automation_summary_hook(self):
        prompt = self._sample_prompt()
        assert "AUTOMATION SUMMARY" in prompt


# ─── Streamlining prompt inlining ─────────────────────────────────────────


class TestBuildStreamliningPromptInlines:
    def _sample_streamlining_prompt(self) -> str:
        from services.wiki_streamlining import (
            StreamliningScope,
            StreamliningTarget,
            build_streamlining_prompt,
        )

        target = StreamliningTarget(
            knowledge_base="example-kb",
            knowledge_base_id="00000000-0000-0000-0000-000000000000",
            provider_api_key="",
            provider="openrouter",
            model="test-model",
            prompt="",
            actor_user_id="00000000-0000-0000-0000-000000000001",
            interval_minutes=1440,
            active_release_id="00000000-0000-0000-0000-000000000002",
        )
        scope = StreamliningScope(
            scope_type="full",
            pages=[
                {
                    "page_key": "00000000-0000-0000-0000-000000000003",
                    "path": "/wiki/",
                    "filename": "overview.md",
                    "title": "Overview",
                    "content": "hi",
                    "tags": [],
                    "sort_order": 0,
                    "full_path": "/wiki/overview.md",
                }
            ],
            dirty_paths=[],
        )
        return build_streamlining_prompt(target, scope)

    def test_streamlining_prompt_contains_full_guide_signature(self):
        prompt = self._sample_streamlining_prompt()
        for must_have in [
            "/wiki/concepts/",
            "/wiki/entities/",
            "Citations — REQUIRED",
            "Visual Elements — MANDATORY",
        ]:
            assert must_have in prompt, f"streamlining prompt missing {must_have!r}"

    def test_streamlining_schema_mentions_delete(self):
        prompt = self._sample_streamlining_prompt()
        assert "delete" in prompt, "streamlining schema should advertise the delete op"


# ─── tool_guide behavior ──────────────────────────────────────────────────


class TestToolGuideReturnsFullDoctrine:
    def test_tool_guide_returns_full_guide_plus_kb_stats(self):
        from services.compile_tools import ToolContext, tool_guide

        class StubPool:
            async def fetchrow(self, query, *args):
                if "knowledge_base_memberships" in query:
                    return {
                        "id": "kb-id",
                        "slug": "example-kb",
                        "name": "Example KB",
                        "role": "owner",
                        "owner_user_id": "user-id",
                    }
                if "SELECT " in query and "source_count" in query:
                    return {"source_count": 42, "wiki_count": 17}
                return None

            async def fetch(self, *_, **__):
                return []

            async def execute(self, *_, **__):
                return "OK"

        ctx = ToolContext(
            pool=StubPool(),
            user_id="user-id",
            knowledge_base_slug="example-kb",
            wiki_release_id=None,
        )
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(tool_guide(ctx))
        finally:
            loop.close()

        assert result.startswith("# LLM Wiki — How It Works"), (
            f"tool_guide did not return the full guide; got {result[:80]!r}"
        )
        assert "Citations — REQUIRED" in result
        assert "/wiki/concepts/" in result
        assert "Example KB" in result
        assert "`example-kb`" in result
        assert "42 sources" in result
        assert "17 wiki pages" in result
