from base64 import b64encode
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from infra.tus import _uploads, tus_create


def _metadata_header(**entries: str) -> str:
    return ",".join(
        f"{key} {b64encode(value.encode('utf-8')).decode('ascii')}"
        for key, value in entries.items()
    )


class FakePool:
    def __init__(self, wiki_direct_editing_enabled: bool):
        self.wiki_direct_editing_enabled = wiki_direct_editing_enabled

    async def fetchrow(self, sql, *args):
        if "storage_limit_bytes" in sql:
            return {"storage_limit_bytes": 1024 * 1024}
        return None

    async def fetchval(self, sql, *args):
        if "wiki_direct_editing_enabled" in sql:
            return self.wiki_direct_editing_enabled
        if "SUM(file_size)" in sql:
            return 0
        return None


def _request(pool: FakePool):
    return SimpleNamespace(
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "128",
            "Upload-Metadata": _metadata_header(
                filename="diagram.pdf",
                knowledge_base_id="11111111-1111-1111-1111-111111111111",
                path="/wiki/",
            ),
        },
        app=SimpleNamespace(state=SimpleNamespace(pool=pool)),
    )


@pytest.mark.asyncio
async def test_tus_create_blocks_wiki_uploads_when_direct_editing_disabled(monkeypatch):
    pool = FakePool(wiki_direct_editing_enabled=False)
    request = _request(pool)

    async def fake_get_user_id(_request):
        return "user-1"

    async def fake_require(*args, **kwargs):
        return {"id": args[1], "slug": "kb", "owner_user_id": "owner-1", "role": "owner"}

    monkeypatch.setattr("infra.tus._get_user_id", fake_get_user_id)
    monkeypatch.setattr("infra.tus.require_kb_role", fake_require)

    with pytest.raises(HTTPException) as exc_info:
        await tus_create(request)

    assert exc_info.value.status_code == 403
    assert "Direct wiki editing is disabled" in exc_info.value.detail
    assert _uploads == {}


@pytest.mark.asyncio
async def test_tus_create_allows_wiki_uploads_when_direct_editing_enabled(monkeypatch):
    pool = FakePool(wiki_direct_editing_enabled=True)
    request = _request(pool)

    async def fake_get_user_id(_request):
        return "user-1"

    async def fake_require(*args, **kwargs):
        return {"id": args[1], "slug": "kb", "owner_user_id": "owner-1", "role": "owner"}

    monkeypatch.setattr("infra.tus._get_user_id", fake_get_user_id)
    monkeypatch.setattr("infra.tus.require_kb_role", fake_require)

    response = await tus_create(request)

    assert response.status_code == 201
    location = response.headers["Location"]
    upload_id = location.rsplit("/", 1)[-1]
    assert _uploads[upload_id].path == "/wiki/"

    _uploads[upload_id].temp_path.unlink(missing_ok=True)
    _uploads.pop(upload_id, None)
