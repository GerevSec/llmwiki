"""Integration tests for KB guidelines and wiki page comments (T1-T6 + CRUD)."""

import pytest

from services.encryption import encrypt_secret
from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, USER_A_ID

# Stable page-key UUIDs used across tests
PAGE_KEY = "cccccccc-cccc-cccc-cccc-cccccccccccc"
ORPHAN_KEY = "dddddddd-dddd-dddd-dddd-dddddddddddd"
RELEASE_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"


@pytest.fixture(autouse=True)
async def seed(pool, monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "ENABLE_KB_GUIDELINES_COMMENTS", True)

    await pool.execute("DELETE FROM wiki_page_comments")
    await pool.execute("DELETE FROM kb_guidelines")
    await pool.execute("DELETE FROM wiki_release_pages")
    await pool.execute("DELETE FROM wiki_releases")
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
        "compile_max_sources, compile_prompt, compile_max_tool_rounds, compile_max_tokens, "
        "provider_secret_encrypted, next_run_at, updated_by) "
        "VALUES ($1, true, 'openrouter', 'anthropic/claude-sonnet-4.6', 60, 5, '', 50, 50000, $2, now(), $3)",
        KB_A_ID,
        encrypt_secret("provider-secret"),
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO documents "
        "(id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ('aaaaaaaa-1111-1111-1111-111111111111', $1, $2, 'source.md', 'Source', '/', 'md', 'ready', 'A useful source', 1)",
        KB_A_ID,
        USER_A_ID,
    )
    yield


# ─── Helpers ─────────────────────────────────────────────────────────────────


async def _seed_active_release(pool, release_id: str, page_key: str | None = None) -> None:
    """Insert a published release, optionally with one page, and set it as active."""
    await pool.execute(
        "INSERT INTO wiki_releases (id, knowledge_base_id, status, created_by) "
        "VALUES ($1::uuid, $2::uuid, 'published', 'test')",
        release_id,
        KB_A_ID,
    )
    if page_key:
        await pool.execute(
            "INSERT INTO wiki_release_pages (release_id, page_key, path, filename, content) "
            "VALUES ($1::uuid, $2::uuid, '/wiki/', 'overview.md', '# Overview')",
            release_id,
            page_key,
        )
    await pool.execute(
        "UPDATE knowledge_base_settings SET active_wiki_release_id = $1::uuid WHERE knowledge_base_id = $2::uuid",
        release_id,
        KB_A_ID,
    )


async def _seed_comment(pool, *, page_key: str, status: str = "open") -> str:
    """Insert a comment and return its UUID as a string."""
    row = await pool.fetchrow(
        "INSERT INTO wiki_page_comments (kb_id, page_key, body, status, author_id) "
        "VALUES ($1::uuid, $2::uuid, 'Fix the intro', $3, $4::uuid) "
        "RETURNING id::text",
        KB_A_ID,
        page_key,
        status,
        USER_A_ID,
    )
    return row["id"]


# ─── Guideline CRUD ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_guideline_crud_lifecycle(client):
    # Initially empty
    resp = await client.get(f"/api/kb/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert resp.json() == []

    # Create
    resp = await client.post(
        f"/api/kb/{KB_A_ID}/guidelines",
        headers=auth_headers(USER_A_ID),
        json={"body": "Always cite sources"},
    )
    assert resp.status_code == 201
    guideline = resp.json()
    gid = guideline["id"]
    assert guideline["body"] == "Always cite sources"
    assert guideline["is_active"] is True

    # List includes it
    resp = await client.get(f"/api/kb/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    # Patch body
    resp = await client.patch(
        f"/api/kb/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
        json={"body": "Always cite primary sources"},
    )
    assert resp.status_code == 200
    assert resp.json()["body"] == "Always cite primary sources"

    # Deactivate
    resp = await client.patch(
        f"/api/kb/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
        json={"is_active": False},
    )
    assert resp.status_code == 200
    assert resp.json()["is_active"] is False

    # Delete (archive)
    resp = await client.delete(
        f"/api/kb/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 204

    # No longer in list
    resp = await client.get(f"/api/kb/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert resp.json() == []


# ─── T1: open → delivered via compile ────────────────────────────────────────


@pytest.mark.asyncio
async def test_t1_compile_delivers_open_comments(client, pool, monkeypatch):
    await _seed_active_release(pool, RELEASE_ID, page_key=PAGE_KEY)
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    async def fake_invoke(prompt, target):
        return {
            "stop_reason": "stop",
            "request_id": "req-t1",
            "text_excerpt": "AUTOMATION SUMMARY\n- Updated wiki",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    resp = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "succeeded"

    row = await pool.fetchrow(
        "SELECT status, delivered_compile_id FROM wiki_page_comments WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "delivered"
    assert row["delivered_compile_id"] is not None


# ─── T2: delivered → resolved ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_t2_resolve_delivered_comment(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="delivered")

    resp = await client.post(
        f"/api/kb/{KB_A_ID}/comments/{comment_id}/resolve",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "resolved"
    assert body["resolved_at"] is not None


# ─── T3: delivered → archived ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_t3_archive_delivered_comment(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="delivered")

    resp = await client.post(
        f"/api/kb/{KB_A_ID}/comments/{comment_id}/archive",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"


# ─── T4: delivered → promoted, guideline created ──────────────────────────────


@pytest.mark.asyncio
async def test_t4_promote_delivered_comment_creates_guideline(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="delivered")

    resp = await client.post(
        f"/api/kb/{KB_A_ID}/comments/{comment_id}/promote",
        headers=auth_headers(USER_A_ID),
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "promoted"
    assert body["promoted_to_guideline_id"] is not None

    guideline_count = await pool.fetchval(
        "SELECT COUNT(*) FROM kb_guidelines WHERE kb_id = $1::uuid AND archived_at IS NULL",
        KB_A_ID,
    )
    assert guideline_count == 1


# ─── T5: orphan page_key → lazy archival on GET ───────────────────────────────


@pytest.mark.asyncio
async def test_t5_orphan_comment_archived_on_fetch(client, pool):
    # Active release has PAGE_KEY but NOT ORPHAN_KEY
    await _seed_active_release(pool, RELEASE_ID, page_key=PAGE_KEY)
    comment_id = await _seed_comment(pool, page_key=ORPHAN_KEY, status="open")

    resp = await client.get(
        f"/api/kb/{KB_A_ID}/pages/{ORPHAN_KEY}/comments",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    assert resp.json() == []

    row = await pool.fetchrow(
        "SELECT status, system_note FROM wiki_page_comments WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "archived"
    assert row["system_note"] == "orphaned"


# ─── T6: open comment → resolve → 409 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_t6_resolve_open_comment_returns_409(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    resp = await client.post(
        f"/api/kb/{KB_A_ID}/comments/{comment_id}/resolve",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 409
