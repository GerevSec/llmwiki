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
async def test_compile_ignores_empty_ready_sources(client, pool, monkeypatch):
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('bbbbbbbb-aaaa-1111-1111-111111111111', $1, $2, 'empty.md', 'Empty', '/', 'md', 'ready', '', 1)",
        KB_A_ID,
        USER_A_ID,
    )

    captured = {}

    async def fake_invoke(prompt, target):
        captured["pending_source_paths"] = target.pending_source_paths
        return {
            "stop_reason": "stop",
            "request_id": "req-nonempty-only",
            "text_excerpt": "AUTOMATION SUMMARY\n- Updated wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-now",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    assert captured["pending_source_paths"] == ("/source.md",)


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
async def test_compile_runs_parses_jsonb_telemetry_for_response_model(client, pool):
    await pool.execute(
        "INSERT INTO compile_runs "
        "(knowledge_base_id, user_id, status, model, provider, source_count, source_snapshot, response_excerpt, telemetry, started_at, finished_at) "
        "VALUES ($1, $2, 'failed', 'openrouter/test', 'openrouter', 1, '[]'::jsonb, NULL, $3::jsonb, now(), now())",
        KB_A_ID,
        USER_A_ID,
        '{"tool_calls": 2, "tool_rounds": 1, "provider_request_ids": ["req-1"]}',
    )

    response = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-runs?limit=5",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    body = response.json()
    assert body[0]["telemetry"] == {
        "tool_calls": 2,
        "tool_rounds": 1,
        "provider_request_ids": ["req-1"],
    }


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


@pytest.mark.asyncio
async def test_compile_due_marks_stale_running_runs_failed(client, pool, monkeypatch):
    await pool.execute(
        "INSERT INTO compile_runs (knowledge_base_id, user_id, status, model, provider, source_count, source_snapshot, started_at, telemetry) "
        "VALUES ($1, $2, 'running', 'stale-model', 'openrouter', 0, '[]'::jsonb, now() - interval '2 hours', '{}'::jsonb)",
        KB_A_ID,
        USER_A_ID,
    )

    async def fake_invoke(prompt, target):
        return {
            "stop_reason": "stop",
            "request_id": "req-stale",
            "text_excerpt": "AUTOMATION SUMMARY\n- Updated wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    response = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )

    assert response.status_code == 200
    stale = await pool.fetchrow(
        "SELECT status, error_message, finished_at FROM compile_runs WHERE model = 'stale-model' ORDER BY started_at DESC LIMIT 1"
    )
    assert stale["status"] == "failed"
    assert "marked stale" in stale["error_message"].lower()
    assert stale["finished_at"] is not None


@pytest.mark.asyncio
async def test_recompile_from_scratch_resets_checkpoints_and_rebuilds_wiki(client, pool, monkeypatch):
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('bbbbbbbb-1111-1111-1111-111111111111', $1, $2, 'overview.md', 'Overview', '/wiki/', 'md', 'ready', '# Old wiki', 1)",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO compiled_source_checkpoints (knowledge_base_id, document_id, compiled_version) VALUES ($1, 'aaaaaaaa-1111-1111-1111-111111111111', 1)",
        KB_A_ID,
    )

    async def fake_invoke(prompt, target):
        return {
            "stop_reason": "stop",
            "request_id": "req-reset",
            "text_excerpt": "AUTOMATION SUMMARY\n- Rebuilt wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/recompile-from-scratch",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert response.json()["reset_source_count"] == 1

    checkpoint_count = await pool.fetchval(
        "SELECT COUNT(*) FROM compiled_source_checkpoints WHERE knowledge_base_id = $1",
        KB_A_ID,
    )
    active_release = await pool.fetchval(
        "SELECT active_wiki_release_id FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        KB_A_ID,
    )
    active_release_pages = await pool.fetchval(
        "SELECT COUNT(*) FROM wiki_release_pages WHERE release_id = $1",
        active_release,
    )
    assert checkpoint_count == 1
    assert active_release is not None
    assert active_release_pages >= 0


@pytest.mark.asyncio
async def test_recompile_from_scratch_batches_large_resets(client, pool, monkeypatch):
    extra_sources = [
        ("bbbbbbbb-2222-1111-1111-111111111111", "source-2.md", "Source 2", "Second source"),
        ("cccccccc-2222-1111-1111-111111111111", "source-3.md", "Source 3", "Third source"),
    ]
    for doc_id, filename, title, content in extra_sources:
        await pool.execute(
            "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
            "VALUES ($1, $2, $3, $4, $5, '/', 'md', 'ready', $6, 1)",
            doc_id,
            KB_A_ID,
            USER_A_ID,
            filename,
            title,
            content,
        )

    batch_paths: list[tuple[str, ...]] = []
    release_ids: list[str | None] = []

    async def fake_invoke(prompt, target):
        batch_paths.append(target.pending_source_paths)
        release_ids.append(target.wiki_release_id)
        return {
            "stop_reason": "stop",
            "request_id": f"req-batch-{len(batch_paths)}",
            "text_excerpt": f"AUTOMATION SUMMARY\n- Batch {len(batch_paths)} complete",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)
    monkeypatch.setattr("services.periodic_compile.settings.LLMWIKI_RECOMPILE_BATCH_MAX_SOURCES", 2)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/recompile-from-scratch",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert [len(batch) for batch in batch_paths] == [2, 1]
    assert len(set(release_ids)) == 1

    checkpoint_count = await pool.fetchval(
        "SELECT COUNT(*) FROM compiled_source_checkpoints WHERE knowledge_base_id = $1",
        KB_A_ID,
    )
    assert checkpoint_count == 3


@pytest.mark.asyncio
async def test_recompile_from_scratch_clears_stale_required_page_links(client, pool, monkeypatch):
    stale_page_ids = [
        "bbbbbbbb-1111-1111-1111-111111111111",
        "cccccccc-1111-1111-1111-111111111111",
        "dddddddd-1111-1111-1111-111111111111",
        "eeeeeeee-1111-1111-1111-111111111111",
        "ffffffff-1111-1111-1111-111111111111",
    ]
    stale_links = "\n".join(
        f"- [Stale {idx}](/wiki/stale/topic-{idx}.md)"
        for idx in range(1, 6)
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('99999999-1111-1111-1111-111111111111', $1, $2, 'overview.md', 'Overview', '/wiki/', 'md', 'ready', $3, 1)",
        KB_A_ID,
        USER_A_ID,
        f'# Overview\n\nLegacy topic map:\n{stale_links}',
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('88888888-1111-1111-1111-111111111111', $1, $2, 'log.md', 'Log', '/wiki/', 'md', 'ready', '# Log\n\nLegacy compile log.', 1)",
        KB_A_ID,
        USER_A_ID,
    )
    for idx, page_id in enumerate(stale_page_ids, start=1):
        await pool.execute(
            "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
            "VALUES ($1, $2, $3, $4, $5, '/wiki/stale/', 'md', 'ready', $6, 1)",
            page_id,
            KB_A_ID,
            USER_A_ID,
            f"topic-{idx}.md",
            f"Stale {idx}",
            f"# Stale {idx}",
        )

    async def fake_invoke(prompt, target):
        return {
            "stop_reason": "stop",
            "request_id": "req-reset-links",
            "text_excerpt": "AUTOMATION SUMMARY\n- Reset wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/recompile-from-scratch",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    overview_content = await pool.fetchval(
        "SELECT content FROM documents WHERE knowledge_base_id = $1 AND path = '/wiki/' AND filename = 'overview.md'",
        KB_A_ID,
    )
    log_content = await pool.fetchval(
        "SELECT content FROM documents WHERE knowledge_base_id = $1 AND path = '/wiki/' AND filename = 'log.md'",
        KB_A_ID,
    )
    assert overview_content == "# Overview\n\n"
    assert log_content == "# Log\n\n"
