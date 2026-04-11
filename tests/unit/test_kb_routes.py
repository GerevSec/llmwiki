from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from routes.knowledge_bases import InviteAccept, UpdateCompileSchedule, accept_knowledge_base_invite, update_compile_schedule


class FakeConn:
    def __init__(self):
        self.invite = {
            "id": "invite-1",
            "knowledge_base_id": "11111111-1111-1111-1111-111111111111",
            "email": "user@example.com",
            "role": "editor",
            "status": "pending",
            "expires_at": datetime.now(UTC) + timedelta(days=1),
        }
        self.executed = []

    async def fetchrow(self, sql, *args):
        if "FROM knowledge_base_invites" in sql:
            return self.invite
        if "FROM knowledge_bases kb" in sql:
            return {
                "id": self.invite["knowledge_base_id"],
                "user_id": "owner-1",
                "name": "KB",
                "slug": "kb",
                "role": "editor",
                "description": None,
                "source_count": 1,
                "wiki_page_count": 2,
                "created_at": __import__("datetime").datetime.now(__import__("datetime").UTC),
                "updated_at": __import__("datetime").datetime.now(__import__("datetime").UTC),
            }
        return None

    async def fetchval(self, sql, *args):
        return "user@example.com"

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "OK"

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def start(self):
        return None

    async def commit(self):
        return None

    async def rollback(self):
        return None


class FakePool:
    def __init__(self, conn):
        self.conn = conn
        self.fetchrow_calls = []
        self.secret = "existing-secret"

    async def acquire(self):
        return self.conn

    async def release(self, conn):
        return None

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        return {
            "enabled": True,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
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
async def test_accept_invite_allows_acceptance_by_invite_id():
    conn = FakeConn()
    pool = FakePool(conn)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool)))

    result = await accept_knowledge_base_invite(
        InviteAccept(invite_id="invite-1"),
        "user-1",
        request,
    )

    assert result["slug"] == "kb"
    assert any("knowledge_base_memberships" in sql for sql, _ in conn.executed)


@pytest.mark.asyncio
async def test_update_compile_schedule_encrypts_secret(monkeypatch):
    pool = FakePool(FakeConn())
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
        ),
        "owner-1",
        request,
    )

    sql, args = pool.fetchrow_calls[0]
    assert "knowledge_base_settings" in sql
    assert "enc::secret-value" in args
    assert result["provider"] == "openrouter"


@pytest.mark.asyncio
async def test_update_compile_schedule_requires_secret_when_enabling(monkeypatch):
    pool = FakePool(FakeConn())
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
            ),
            "owner-1",
            request,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Provider secret is required before enabling periodic compile"
