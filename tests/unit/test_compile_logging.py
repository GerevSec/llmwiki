"""Unit tests for compile/streamline structured logging.

These tests never call real providers or databases. They validate:
- Log records emit under the llmwiki.compile / llmwiki.streamline loggers.
- Sensitive fields are stripped from structured fields.
- wiki_streamlining imports rename_release_page (regression for missing import).
"""

import logging
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))

from services.compile_logging import (  # noqa: E402
    COMPILE_LOGGER_NAME,
    STREAMLINE_LOGGER_NAME,
    log_compile,
    log_streamline,
    preview,
)


class TestPreview:
    def test_truncates_long_strings(self):
        out = preview("x" * 500, limit=40)
        assert out.startswith("x" * 40)
        assert "+" in out  # length overflow marker

    def test_serializes_dict_and_drops_secrets(self):
        out = preview({"api_key": "secret", "tool": "read", "path": "/wiki/a.md"})
        assert "api_key" not in out
        assert "secret" not in out
        assert "read" in out
        assert "/wiki/a.md" in out

    def test_handles_none_and_non_strings(self):
        assert preview(None) == ""
        assert "42" in preview(42)

    def test_strips_newlines(self):
        assert "\n" not in preview("line1\nline2")


class TestLogCompile:
    def test_emits_info_record_with_event_and_fields(self, caplog):
        caplog.set_level(logging.INFO, logger=COMPILE_LOGGER_NAME)
        log_compile(
            "run_start",
            kb="bes-wiki",
            provider="openrouter",
            model="minimax/minimax-m2.7",
            source_count=18,
        )
        assert len(caplog.records) >= 1
        message = caplog.records[-1].getMessage()
        assert "event=run_start" in message
        assert "kb=" in message and "bes-wiki" in message
        assert "provider=openrouter" in message
        assert "source_count=18" in message

    def test_drops_sensitive_keys(self, caplog):
        caplog.set_level(logging.INFO, logger=COMPILE_LOGGER_NAME)
        log_compile("provider_request", kb="bes-wiki", api_key="sk-secret", authorization="Bearer abc")
        message = caplog.records[-1].getMessage()
        assert "sk-secret" not in message
        assert "Bearer" not in message
        assert "api_key" not in message

    def test_skips_empty_values(self, caplog):
        caplog.set_level(logging.INFO, logger=COMPILE_LOGGER_NAME)
        log_compile("provider_response", kb="bes-wiki", request_id=None, stop_reason="")
        message = caplog.records[-1].getMessage()
        assert "request_id" not in message
        assert "stop_reason" not in message


class TestLogStreamline:
    def test_emits_under_streamline_logger(self, caplog):
        caplog.set_level(logging.INFO, logger=STREAMLINE_LOGGER_NAME)
        log_streamline("scope_determined", kb="bes-wiki", scope_type="full", pages_in_scope=16)
        assert len(caplog.records) >= 1
        record = caplog.records[-1]
        assert record.name == STREAMLINE_LOGGER_NAME
        assert "event=scope_determined" in record.getMessage()
        assert "scope_type=full" in record.getMessage()


class TestWikiStreamliningImport:
    def test_rename_release_page_is_importable(self):
        """Regression: wiki_streamlining.py must import rename_release_page so that
        streamlining rename/move operations do not raise NameError."""
        from services import wiki_streamlining

        assert hasattr(wiki_streamlining, "rename_release_page"), (
            "wiki_streamlining must expose rename_release_page via its module imports "
            "(missing import caused NameError in prod streamlining runs)"
        )

    def test_apply_streamlining_rename_raises_clear_error_not_nameerror(self):
        """apply_streamlining_operations against a stubbed conn must raise a RuntimeError
        about a missing source page (NOT a NameError), proving the import fix holds."""
        import asyncio

        from services.wiki_streamlining import StreamliningTarget, apply_streamlining_operations

        target = StreamliningTarget(
            knowledge_base="unit-test-kb",
            knowledge_base_id="00000000-0000-0000-0000-000000000000",
            provider_api_key="",
            provider="openrouter",
            model="test",
            prompt="",
            actor_user_id="00000000-0000-0000-0000-000000000001",
            interval_minutes=1440,
            active_release_id="00000000-0000-0000-0000-000000000002",
        )

        class StubConn:
            async def fetch(self, *_, **__):
                return []
            async def fetchrow(self, *_, **__):
                return None
            async def fetchval(self, *_, **__):
                return None
            async def execute(self, *_, **__):
                return "OK"

        operations = [
            {
                "type": "rename",
                "source_path": "/wiki/topic-a/alpha.md",
                "target_path": "/wiki/topic-a/alpha-renamed.md",
            }
        ]

        with pytest.raises(RuntimeError) as excinfo:
            asyncio.get_event_loop().run_until_complete(
                apply_streamlining_operations(StubConn(), target, "00000000-0000-0000-0000-000000000002", operations)
            ) if False else asyncio.new_event_loop().run_until_complete(
                apply_streamlining_operations(StubConn(), target, "00000000-0000-0000-0000-000000000002", operations)
            )

        assert "NameError" not in str(excinfo.value)
        assert "rename_release_page" not in str(excinfo.value)
        # Should fail cleanly on missing source page, not on missing function
        assert "Source page not found" in str(excinfo.value) or "not found" in str(excinfo.value)
