from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from routes.documents import CreateNote, _ensure_wiki_direct_editable, create_note


class FakePool:
    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.fetchval_calls: list[tuple[str, tuple]] = []

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        return self.enabled


@pytest.mark.asyncio
async def test_wiki_editability_guard_skips_non_wiki_paths():
    pool = FakePool(enabled=False)

    await _ensure_wiki_direct_editable(pool, "kb-1", path="/notes/")

    assert pool.fetchval_calls == []


@pytest.mark.asyncio
async def test_wiki_editability_guard_blocks_wiki_paths_when_disabled():
    pool = FakePool(enabled=False)

    with pytest.raises(HTTPException) as exc_info:
        await _ensure_wiki_direct_editable(pool, "kb-1", path="/wiki/", detail="blocked")

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "blocked"


@pytest.mark.asyncio
async def test_create_note_rejects_direct_wiki_creation_when_disabled(monkeypatch):
    pool = FakePool(enabled=False)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(pool=pool)))

    async def fake_require(*args, **kwargs):
        return {"id": args[1], "slug": "kb", "owner_user_id": "owner-1", "role": "owner"}

    monkeypatch.setattr("routes.documents.require_kb_role", fake_require)

    with pytest.raises(HTTPException) as exc_info:
        await create_note(
            "11111111-1111-1111-1111-111111111111",
            CreateNote(filename="overview.md", path="/wiki/", content="# Overview"),
            "owner-1",
            request,
        )

    assert exc_info.value.status_code == 403
    assert "Direct wiki editing is disabled" in exc_info.value.detail
