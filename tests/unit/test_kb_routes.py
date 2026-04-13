from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from routes.knowledge_bases import accept_knowledge_base_invite, update_compile_schedule, UpdateCompileSchedule


class FakePool:
    def __init__(self):
        self.fetchrow_calls = []
        self.secret = "existing-secret"

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        return {
            "enabled": True,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "wiki_direct_editing_enabled": True,
            "interval_minutes": 60,
            "max_sources": 3,
            "prompt": "",
            "max_tool_rounds": 50,
            "max_tokens": 50000,
            "has_provider_secret": True,
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
            "next_run_at": None,
        }

    async def fetchval(self, sql, *args):
        if "provider_secret_encrypted" in sql:
            return self.secret
        return None


@pytest.mark.asyncio
async def test_accept_invite_is_gone():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=FakePool())))

    with pytest.raises(HTTPException) as exc_info:
        await accept_knowledge_base_invite({}, "user-1", request)

    assert exc_info.value.status_code == 410
    assert "no longer used" in exc_info.value.detail


@pytest.mark.asyncio
async def test_update_compile_schedule_encrypts_secret(monkeypatch):
    pool = FakePool()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool)))

    async def fake_require(*args, **kwargs):
        return {"id": args[1], "slug": "kb", "owner_user_id": "owner-1", "role": "owner"}

    monkeypatch.setattr("routes.knowledge_bases.require_kb_role", fake_require)
    monkeypatch.setattr("routes.knowledge_bases.encrypt_secret", lambda value: f"enc::{value}")

    result = await update_compile_schedule(
        "11111111-1111-1111-1111-111111111111",
        UpdateCompileSchedule(
            enabled=True,
            provider="openrouter",
            model="anthropic/claude-sonnet-4.6",
            interval_minutes=60,
            max_sources=2,
            prompt=None,
            provider_secret="secret-value",
            max_tool_rounds=50,
            max_tokens=50000,
            wiki_direct_editing_enabled=True,
        ),
        "owner-1",
        request,
    )

    sql, args = pool.fetchrow_calls[0]
    assert "knowledge_base_settings" in sql
    assert "enc::secret-value" in args
    assert True in args
    assert result["provider"] == "openrouter"


@pytest.mark.asyncio
async def test_update_compile_schedule_requires_secret_when_enabling(monkeypatch):
    pool = FakePool()
    pool.secret = None
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool)))

    async def fake_require(*args, **kwargs):
        return {"id": args[1], "slug": "kb", "owner_user_id": "owner-1", "role": "owner"}

    monkeypatch.setattr("routes.knowledge_bases.require_kb_role", fake_require)

    with pytest.raises(HTTPException) as exc_info:
        await update_compile_schedule(
            "11111111-1111-1111-1111-111111111111",
            UpdateCompileSchedule(
                enabled=True,
                provider="openrouter",
                model="anthropic/claude-sonnet-4.6",
                interval_minutes=60,
                max_sources=20,
                provider_secret=None,
                max_tool_rounds=50,
                max_tokens=50000,
                wiki_direct_editing_enabled=False,
            ),
            "owner-1",
            request,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Provider secret is required before enabling periodic compile"
