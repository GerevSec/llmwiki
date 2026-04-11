import pytest

from services.encryption import encrypt_secret
from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, USER_A_ID


@pytest.fixture(autouse=True)
async def seed_compile_workflow(pool):
    await pool.execute("DELETE FROM document_chunks")
    await pool.execute("DELETE FROM document_pages")
    await pool.execute("DELETE FROM documents")
    await pool.execute("DELETE FROM knowledge_base_invites")
    await pool.execute("DELETE FROM knowledge_base_memberships")
    await pool.execute("DELETE FROM knowledge_base_settings")
    await pool.execute("DELETE FROM compile_runs")
    await pool.execute("DELETE FROM compiled_source_checkpoints")
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
        "INSERT INTO knowledge_base_settings "
        "(knowledge_base_id, auto_compile_enabled, compile_provider, compile_model, compile_interval_minutes, "
        "compile_max_sources, compile_prompt, compile_max_tool_rounds, compile_max_tokens, provider_secret_encrypted, next_run_at, updated_by) "
        "VALUES ($1, true, 'openrouter', 'anthropic/claude-sonnet-4.6', 60, 5, '', 50, 50000, $2, now(), $3)",
        KB_A_ID,
        encrypt_secret("provider-secret"),
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('aaaaaaaa-1111-1111-1111-111111111111', $1, $2, 'source.md', 'Source', '/', 'md', 'ready', 'A useful source', 1)",
        KB_A_ID,
        USER_A_ID,
    )

    yield


@pytest.mark.asyncio
async def test_internal_compile_due_runs_then_skips(client, pool, monkeypatch):
    async def fake_invoke(prompt, target):
        return {
            "stop_reason": "stop",
            "request_id": "req-123",
            "text_excerpt": "AUTOMATION SUMMARY\n- Updated wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    first = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert first.status_code == 200
    payload = first.json()
    assert payload[0]["status"] == "succeeded"
    assert payload[0]["knowledge_base"] == "alice-kb"

    run = await pool.fetchrow(
        "SELECT status, provider, source_count FROM compile_runs ORDER BY started_at DESC LIMIT 1"
    )
    assert run["status"] == "succeeded"
    assert run["provider"] == "openrouter"
    assert run["source_count"] == 1

    checkpoint = await pool.fetchrow(
        "SELECT compiled_version FROM compiled_source_checkpoints WHERE knowledge_base_id = $1",
        KB_A_ID,
    )
    assert checkpoint["compiled_version"] == 1

    second = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert second.status_code == 200
    second_payload = second.json()
    assert second_payload == []


@pytest.mark.asyncio
async def test_compile_now_uses_kb_settings(client, monkeypatch):
    async def fake_invoke(prompt, target):
        assert target.provider == "openrouter"
        assert target.model == "anthropic/claude-sonnet-4.6"
        assert target.provider_api_key == "provider-secret"
        return {
            "stop_reason": "stop",
            "request_id": "req-456",
            "text_excerpt": "AUTOMATION SUMMARY\n- Compile now ran",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-now",
        headers=auth_headers(USER_A_ID),
    )
    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"


@pytest.mark.asyncio
async def test_compile_schedule_defaults_are_high_enough(client):
    response = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_A_ID),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["max_tool_rounds"] == 50
    assert body["max_tokens"] == 50000


@pytest.mark.asyncio
async def test_enabling_schedule_requires_secret_when_missing(client, pool):
    await pool.execute(
        "UPDATE knowledge_base_settings SET provider_secret_encrypted = NULL WHERE knowledge_base_id = $1",
        KB_A_ID,
    )

    response = await client.put(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_A_ID),
        json={
            "enabled": True,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "interval_minutes": 60,
            "max_sources": 20,
            "max_tool_rounds": 50,
            "max_tokens": 50000,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Provider secret is required before enabling periodic compile"
