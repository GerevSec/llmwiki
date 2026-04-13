import pytest

from services.wiki_releases import ensure_initial_wiki_release
from services.encryption import encrypt_secret
from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, USER_A_ID

OVERVIEW_ID = "aaaaaaaa-1111-1111-1111-111111111111"
LOG_ID = "bbbbbbbb-1111-1111-1111-111111111111"
ALPHA_ID = "cccccccc-1111-1111-1111-111111111111"
BETA_ID = "dddddddd-1111-1111-1111-111111111111"
SOURCE_ID = "eeeeeeee-1111-1111-1111-111111111111"


@pytest.fixture(autouse=True)
async def seed_streamlining(pool):
    await pool.execute("DELETE FROM document_chunks")
    await pool.execute("DELETE FROM document_pages")
    await pool.execute("DELETE FROM documents")
    await pool.execute("DELETE FROM wiki_path_aliases")
    await pool.execute("DELETE FROM wiki_release_pages")
    await pool.execute("DELETE FROM wiki_releases")
    await pool.execute("DELETE FROM wiki_dirty_scope")
    await pool.execute("DELETE FROM streamlining_runs")
    await pool.execute("DELETE FROM compile_runs")
    await pool.execute("DELETE FROM compiled_source_checkpoints")
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
        "INSERT INTO knowledge_base_settings (knowledge_base_id, updated_by, wiki_direct_editing_enabled, compile_provider, compile_model, provider_secret_encrypted, streamlining_enabled, streamlining_interval_minutes, next_streamlining_at) "
        "VALUES ($1, $2, true, 'openrouter', 'anthropic/claude-sonnet-4.6', $3, true, 1440, now())",
        KB_A_ID,
        USER_A_ID,
        encrypt_secret('provider-secret'),
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) VALUES ($1, $2, $3, 'overview.md', 'Overview', '/wiki/', 'md', 'ready', $4, 1)",
        OVERVIEW_ID,
        KB_A_ID,
        USER_A_ID,
        '# Overview\n\nAlpha and beta concepts.',
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) VALUES ($1, $2, $3, 'log.md', 'Log', '/wiki/', 'md', 'ready', $4, 1)",
        LOG_ID,
        KB_A_ID,
        USER_A_ID,
        '# Log\n\nCreated wiki.',
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) VALUES ($1, $2, $3, 'alpha.md', 'Alpha', '/wiki/topic-a/', 'md', 'ready', $4, 1)",
        ALPHA_ID,
        KB_A_ID,
        USER_A_ID,
        '# Alpha\n\nShared fact one.\n\nUnique alpha fact.\n\n[Beta](/wiki/topic-b/beta.md)',
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) VALUES ($1, $2, $3, 'beta.md', 'Beta', '/wiki/topic-b/', 'md', 'ready', $4, 1)",
        BETA_ID,
        KB_A_ID,
        USER_A_ID,
        '# Beta\n\nShared fact one.\n\nUnique beta fact.\n\n[Alpha](/wiki/topic-a/alpha.md)',
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) VALUES ($1, $2, $3, 'notes.md', 'Notes', '/', 'md', 'ready', $4, 1)",
        SOURCE_ID,
        KB_A_ID,
        USER_A_ID,
        '[Alpha](/wiki/topic-a/alpha.md)',
    )

    conn = await pool.acquire()
    try:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM wiki_release_pages WHERE release_id IN (SELECT id FROM wiki_releases WHERE knowledge_base_id = $1)",
                KB_A_ID,
            )
            await conn.execute(
                "DELETE FROM wiki_releases WHERE knowledge_base_id = $1",
                KB_A_ID,
            )
            await conn.execute(
                "UPDATE knowledge_base_settings SET active_wiki_release_id = NULL WHERE knowledge_base_id = $1",
                KB_A_ID,
            )
            await ensure_initial_wiki_release(conn, KB_A_ID)
    finally:
        await pool.release(conn)

    yield


@pytest.mark.asyncio
async def test_wiki_rename_creates_alias_and_rewrites_links(client, pool):
    before_release = await pool.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        KB_A_ID,
    )

    response = await client.patch(
        f"/v1/documents/{ALPHA_ID}",
        headers=auth_headers(USER_A_ID),
        json={"path": "/wiki/merged/", "filename": "alpha-core.md"},
    )

    assert response.status_code == 200
    assert response.json()["path"] == "/wiki/merged/"
    assert response.json()["filename"] == "alpha-core.md"

    after_release = await pool.fetchval(
        "SELECT active_wiki_release_id::text FROM knowledge_base_settings WHERE knowledge_base_id = $1",
        KB_A_ID,
    )
    assert after_release != before_release

    beta_content = await pool.fetchval("SELECT content FROM documents WHERE id = $1", BETA_ID)
    source_content = await pool.fetchval("SELECT content FROM documents WHERE id = $1", SOURCE_ID)
    assert "/wiki/merged/alpha-core.md" in beta_content
    assert "/wiki/merged/alpha-core.md" in source_content

    resolve = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/resolve-wiki-path",
        headers=auth_headers(USER_A_ID),
        params={"path": "/wiki/topic-a/alpha.md"},
    )
    assert resolve.status_code == 200
    assert resolve.json()["path"] == "/wiki/merged/alpha-core.md"


@pytest.mark.asyncio
async def test_streamline_now_merges_duplicate_pages(client, pool, monkeypatch):
    async def fake_invoke(prompt, target):
        return {
            "request_id": "stream-1",
            "text": """
            {
              "summary": "Merged overlapping topic pages.",
              "operations": [
                {
                  "type": "merge",
                  "source_path": "/wiki/topic-b/beta.md",
                  "target_path": "/wiki/topic-a/alpha.md",
                  "reason": "alpha should be the canonical page"
                }
              ]
            }
            """,
        }

    monkeypatch.setattr("services.wiki_streamlining._invoke_streamlining_provider", fake_invoke)

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/streamline-now?force_full=true",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert response.json()["scope_type"] == "full"

    alpha = await pool.fetchrow("SELECT content FROM documents WHERE id = $1 AND archived = false", ALPHA_ID)
    beta = await pool.fetchrow("SELECT archived FROM documents WHERE id = $1", BETA_ID)
    assert "Unique alpha fact." in alpha["content"]
    assert "Unique beta fact." in alpha["content"]
    assert beta["archived"] is True

    resolve = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/resolve-wiki-path",
        headers=auth_headers(USER_A_ID),
        params={"path": "/wiki/topic-b/beta.md"},
    )
    assert resolve.status_code == 200
    assert resolve.json()["path"] == "/wiki/topic-a/alpha.md"

    run = await pool.fetchrow(
        "SELECT status, scope_type FROM streamlining_runs WHERE knowledge_base_id = $1 ORDER BY started_at DESC LIMIT 1",
        KB_A_ID,
    )
    assert run["status"] == "succeeded"
    assert run["scope_type"] == "full"
