from base64 import b64encode

import pytest

from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, USER_A_ID

WIKI_DOC_ID = "bbbbbbbb-1111-1111-1111-111111111111"
SOURCE_DOC_ID = "cccccccc-1111-1111-1111-111111111111"


def tus_metadata(**entries: str) -> str:
    return ",".join(
        f"{key} {b64encode(value.encode('utf-8')).decode('ascii')}"
        for key, value in entries.items()
    )


@pytest.fixture(autouse=True)
async def seed_wiki_editability(pool):
    await pool.execute("DELETE FROM document_chunks")
    await pool.execute("DELETE FROM document_pages")
    await pool.execute("DELETE FROM documents")
    await pool.execute("DELETE FROM knowledge_base_invites")
    await pool.execute("DELETE FROM knowledge_base_memberships")
    await pool.execute("DELETE FROM knowledge_base_settings")
    await pool.execute("DELETE FROM api_keys")
    await pool.execute("DELETE FROM knowledge_bases")
    await pool.execute("DELETE FROM users")

    await pool.execute(
        "INSERT INTO users (id, email, display_name) VALUES ($1, 'alice@test.com', 'Alice')",
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_bases (id, user_id, name, slug) VALUES ($1, $2, 'Alice KB', 'alice-kb')",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner')",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_settings (knowledge_base_id, updated_by, wiki_direct_editing_enabled) VALUES ($1, $2, false)",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'overview.md', 'Overview', '/wiki/', 'md', 'ready', '# Overview', 1)",
        WIKI_DOC_ID,
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'notes.md', 'Notes', '/', 'md', 'ready', '# Notes', 1)",
        SOURCE_DOC_ID,
        KB_A_ID,
        USER_A_ID,
    )

    yield


@pytest.mark.asyncio
async def test_compile_schedule_exposes_wiki_editability_flag(client):
    response = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    assert response.json()["wiki_direct_editing_enabled"] is False


@pytest.mark.asyncio
async def test_owner_can_toggle_wiki_editability(client):
    response = await client.put(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_A_ID),
        json={
            "enabled": False,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "wiki_direct_editing_enabled": True,
            "interval_minutes": 60,
            "max_sources": 20,
            "prompt": "",
            "max_tool_rounds": 50,
            "max_tokens": 50000,
        },
    )

    assert response.status_code == 200
    assert response.json()["wiki_direct_editing_enabled"] is True


@pytest.mark.asyncio
async def test_direct_wiki_edit_is_blocked_when_disabled(client):
    response = await client.put(
        f"/v1/documents/{WIKI_DOC_ID}/content",
        headers=auth_headers(USER_A_ID),
        json={"content": "# Changed"},
    )

    assert response.status_code == 403
    assert "Direct wiki editing is disabled" in response.json()["detail"]


@pytest.mark.asyncio
async def test_source_edit_remains_allowed_when_wiki_editing_disabled(client):
    response = await client.put(
        f"/v1/documents/{SOURCE_DOC_ID}/content",
        headers=auth_headers(USER_A_ID),
        json={"content": "# Changed source"},
    )

    assert response.status_code == 200
    assert response.json()["content"] == "# Changed source"


@pytest.mark.asyncio
async def test_direct_wiki_edit_is_allowed_after_enabling(client, pool):
    await pool.execute(
        "UPDATE knowledge_base_settings SET wiki_direct_editing_enabled = true WHERE knowledge_base_id = $1",
        KB_A_ID,
    )

    response = await client.put(
        f"/v1/documents/{WIKI_DOC_ID}/content",
        headers=auth_headers(USER_A_ID),
        json={"content": "# Changed"},
    )

    assert response.status_code == 200
    assert response.json()["content"] == "# Changed"


@pytest.mark.asyncio
async def test_wiki_upload_creation_is_blocked_when_disabled(client):
    response = await client.post(
        "/v1/uploads",
        headers={
            **auth_headers(USER_A_ID),
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "128",
            "Upload-Metadata": tus_metadata(
                filename="diagram.pdf",
                knowledge_base_id=KB_A_ID,
                path="/wiki/",
            ),
        },
    )

    assert response.status_code == 403
    assert "Direct wiki editing is disabled" in response.json()["detail"]


@pytest.mark.asyncio
async def test_wiki_upload_creation_is_allowed_after_enabling(client, pool):
    await pool.execute(
        "UPDATE knowledge_base_settings SET wiki_direct_editing_enabled = true WHERE knowledge_base_id = $1",
        KB_A_ID,
    )

    response = await client.post(
        "/v1/uploads",
        headers={
            **auth_headers(USER_A_ID),
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "128",
            "Upload-Metadata": tus_metadata(
                filename="diagram.pdf",
                knowledge_base_id=KB_A_ID,
                path="/wiki/",
            ),
        },
    )

    assert response.status_code == 201
    assert response.headers["Location"].startswith("/v1/uploads/")
