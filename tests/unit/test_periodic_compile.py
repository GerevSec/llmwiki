from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))
sys.path.append(str(ROOT / "mcp"))

from services.periodic_compile import (  # noqa: E402
    CompileTarget,
    PendingSource,
    _invoke_anthropic,
    _invoke_openrouter,
    build_compile_prompt,
    filter_pending_sources,
    run_target,
)
from services.wiki_streamlining import _extract_json_payload  # noqa: E402
from api_key_auth import hash_api_key  # noqa: E402


class TestPeriodicCompileHelpers:
    def test_filter_pending_sources_skips_unchanged_and_non_source_docs(self):
        now = datetime.now(UTC)
        rows = [
            {
                "id": "1",
                "path": "/",
                "filename": "fresh.pdf",
                "title": "Fresh",
                "status": "ready",
                "archived": False,
                "version": 2,
                "updated_at": now,
            },
            {
                "id": "2",
                "path": "/wiki/",
                "filename": "overview.md",
                "title": "Overview",
                "status": "ready",
                "archived": False,
                "version": 5,
                "updated_at": now,
            },
            {
                "id": "3",
                "path": "/",
                "filename": "processing.pdf",
                "title": "Processing",
                "status": "processing",
                "archived": False,
                "version": 1,
                "updated_at": now - timedelta(minutes=1),
            },
            {
                "id": "4",
                "path": "/",
                "filename": "unchanged.pdf",
                "title": "Unchanged",
                "status": "ready",
                "archived": False,
                "version": 7,
                "updated_at": now - timedelta(minutes=2),
            },
        ]

        pending = filter_pending_sources(rows, {"4": 7}, max_sources=10)

        assert [(source.id, source.full_path) for source in pending] == [("1", "/fresh.pdf")]

    def test_filter_pending_sources_honors_limit_and_sorts_oldest_first(self):
        now = datetime.now(UTC)
        rows = [
            {
                "id": "a",
                "path": "/",
                "filename": "a.pdf",
                "title": "A",
                "status": "ready",
                "archived": False,
                "version": 1,
                "updated_at": now,
            },
            {
                "id": "b",
                "path": "/",
                "filename": "b.pdf",
                "title": "B",
                "status": "ready",
                "archived": False,
                "version": 1,
                "updated_at": now - timedelta(hours=1),
            },
        ]

        pending = filter_pending_sources(rows, {}, max_sources=1)

        assert [source.id for source in pending] == ["b"]

    def test_build_compile_prompt_includes_kb_and_sources(self):
        sources = [
            PendingSource(
                id="1",
                path="/",
                filename="paper.pdf",
                title="Paper",
                version=3,
                updated_at=None,
            )
        ]

        prompt = build_compile_prompt("research", sources, "Prefer concise updates.")

        assert "`research`" in prompt
        assert "`/paper.pdf` (version 3)" in prompt
        assert "AUTOMATION SUMMARY" in prompt
        assert "Prefer concise updates." in prompt

    def test_hash_api_key_is_stable(self):
        assert hash_api_key("sv_test_key") == hash_api_key("sv_test_key")


@pytest.mark.asyncio
async def test_invoke_anthropic_retries_pause_turn_then_succeeds(monkeypatch):
    responses = [
        {
            "stop_reason": "pause_turn",
            "content": [{"type": "text", "text": "Continuing..."}],
            "id": "msg_1",
        },
        {
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "Done"}],
            "id": "msg_2",
        },
    ]

    class FakeResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, headers, json):
            self.calls.append(json["messages"])
            return FakeResponse(responses.pop(0))

    monkeypatch.setattr("services.periodic_compile.httpx.AsyncClient", FakeClient)

    result = await _invoke_anthropic(
        "Compile now",
        CompileTarget("kb", "test-key", "", 10, "anthropic", "claude-test", 4, 1024, "user-1"),
    )

    assert result["stop_reason"] == "end_turn"
    assert result["request_id"] == "msg_2"


@pytest.mark.asyncio
async def test_invoke_anthropic_rejects_non_terminal_stop_reason(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"stop_reason": "refusal", "content": [], "id": "msg_refusal"}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, headers, json):
            return FakeResponse()

    monkeypatch.setattr("services.periodic_compile.httpx.AsyncClient", FakeClient)

    with pytest.raises(RuntimeError, match="stop_reason=refusal"):
        await _invoke_anthropic(
            "Compile now",
            CompileTarget("kb", "test-key", "", 10, "anthropic", "claude-test", 4, 1024, "user-1"),
        )


@pytest.mark.asyncio
async def test_invoke_openrouter_repairs_invalid_tool_argument_json(monkeypatch):
    responses = [
        {
            "id": "or_1",
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "write",
                                    "arguments": '{"command":"create","path":"/wiki/foo.md","content":"The page is called "Foo"."}',
                                },
                            }
                        ],
                    },
                }
            ],
        },
        {
            "id": "or_2",
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"content": "Done"},
                }
            ],
        },
    ]
    captured: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, headers, json):
            return FakeResponse(responses.pop(0))

    class AcquireCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class FakePool:
        def acquire(self):
            return AcquireCtx()

    async def fake_get_pool():
        return FakePool()

    async def fake_execute_tool(context, name, arguments):
        captured["name"] = name
        captured["arguments"] = arguments
        return "ok"

    monkeypatch.setattr("services.periodic_compile.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("services.periodic_compile._get_pool_for_tools", fake_get_pool)
    monkeypatch.setattr("services.periodic_compile.execute_tool", fake_execute_tool)

    result = await _invoke_openrouter(
        "Compile now",
        CompileTarget("kb", "test-key", "", 10, "openrouter", "openrouter/test", 4, 1024, "user-1", wiki_release_id="rel-1"),
    )

    assert captured["name"] == "write"
    assert captured["arguments"] == {
        "command": "create",
        "path": "/wiki/foo.md",
        "content": 'The page is called "Foo".',
    }
    assert result["request_id"] == "or_2"
    assert result["stop_reason"] == "stop"


def test_extract_json_payload_repairs_embedded_quotes_inside_code_fence():
    payload = _extract_json_payload(
        """```json
{"summary":"ok","operations":[{"type":"update","target_path":"/wiki/foo.md","content":"Rename "Foo" to "Bar""}]}
```"""
    )

    assert payload["summary"] == "ok"
    assert payload["operations"][0]["content"] == 'Rename "Foo" to "Bar"'


@pytest.mark.asyncio
async def test_run_target_rejects_overlapping_compile_lock():
    class FakeConn:
        async def fetchrow(self, sql, *args):
            return {"id": "kb-id", "user_id": "user-id", "slug": "kb", "name": "KB"}

        async def fetch(self, sql, *args):
            return []

        async def fetchval(self, sql, *args):
            return False

        async def execute(self, sql, *args):
            return None

    class AcquireCtx:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class FakePool:
        def acquire(self):
            return AcquireCtx(FakeConn())

    with pytest.raises(RuntimeError, match="already running"):
        await run_target(
            FakePool(),
            CompileTarget("kb", "test-key", "", 10, "anthropic", "claude-test", 4, 1024, "user-1"),
        )
