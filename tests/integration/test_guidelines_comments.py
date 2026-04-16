"""Integration tests for KB directives v2 status machine (kb_directives unified table)."""

import pytest
from datetime import datetime, timezone, timedelta

from services.encryption import encrypt_secret
from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, USER_A_ID

# Stable page-key UUIDs used across tests
PAGE_KEY = "cccccccc-cccc-cccc-cccc-cccccccccccc"
ORPHAN_KEY = "dddddddd-dddd-dddd-dddd-dddddddddddd"
RELEASE_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
DOC_ID = "aaaaaaaa-1111-1111-1111-111111111111"


@pytest.fixture(autouse=True)
async def seed(pool, monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "KB_GUIDELINES_COMMENTS_DISABLED", False)

    await pool.execute("DELETE FROM kb_directives")
    await pool.execute("DELETE FROM wiki_release_pages")
    await pool.execute("DELETE FROM wiki_releases")
    await pool.execute("DELETE FROM compiled_source_checkpoints")
    await pool.execute("DELETE FROM compile_runs")
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
        "VALUES ($1::uuid, $2::uuid, $3::uuid, 'source.md', 'Source', '/', 'md', 'ready', 'A useful source', 1)",
        DOC_ID,
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
    """Insert a comment into kb_directives and return its UUID as a string."""
    row = await pool.fetchrow(
        "INSERT INTO kb_directives (kb_id, kind, scope_page_key, body, status, author_id) "
        "VALUES ($1::uuid, 'comment', $2::uuid, 'Fix the intro', $3, $4::uuid) "
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
    resp = await client.get(f"/v1/knowledge-bases/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert resp.json() == []

    # Create
    resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/guidelines",
        headers=auth_headers(USER_A_ID),
        json={"body": "Always cite sources"},
    )
    assert resp.status_code == 201
    guideline = resp.json()
    gid = guideline["id"]
    assert guideline["body"] == "Always cite sources"
    assert guideline["is_active"] is True

    # List includes it
    resp = await client.get(f"/v1/knowledge-bases/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    # Patch body
    resp = await client.patch(
        f"/v1/knowledge-bases/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
        json={"body": "Always cite primary sources"},
    )
    assert resp.status_code == 200
    assert resp.json()["body"] == "Always cite primary sources"

    # Deactivate
    resp = await client.patch(
        f"/v1/knowledge-bases/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
        json={"is_active": False},
    )
    assert resp.status_code == 200
    assert resp.json()["is_active"] is False

    # Delete (archive)
    resp = await client.delete(
        f"/v1/knowledge-bases/{KB_A_ID}/guidelines/{gid}",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 204

    # No longer in list
    resp = await client.get(f"/v1/knowledge-bases/{KB_A_ID}/guidelines", headers=auth_headers(USER_A_ID))
    assert resp.status_code == 200
    assert resp.json() == []


# ─── T1: open → resolved via compile ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_t1_compile_resolves_open_comments(client, pool, monkeypatch):
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
        "SELECT status, compiled_run_id, compiled_at FROM kb_directives WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "resolved"
    assert row["compiled_run_id"] is not None
    assert row["compiled_at"] is not None


# ─── T2: open → archived (admin suppression) ─────────────────────────────────


@pytest.mark.asyncio
async def test_t2_archive_open_comment(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/comments/{comment_id}/archive",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"

    row = await pool.fetchrow(
        "SELECT status FROM kb_directives WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "archived"


# ─── T3: resolved → archived (bookkeeping; wiki unchanged) ───────────────────


@pytest.mark.asyncio
async def test_t3_archive_resolved_comment_is_bookkeeping(client, pool):
    await _seed_active_release(pool, RELEASE_ID, page_key=PAGE_KEY)
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="resolved")

    resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/comments/{comment_id}/archive",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"

    # Active wiki release and its page should be untouched
    release_row = await pool.fetchrow(
        "SELECT status FROM wiki_releases WHERE id = $1::uuid",
        RELEASE_ID,
    )
    assert release_row["status"] == "published"

    page_row = await pool.fetchrow(
        "SELECT content FROM wiki_release_pages WHERE release_id = $1::uuid AND page_key = $2::uuid",
        RELEASE_ID,
        PAGE_KEY,
    )
    assert page_row["content"] == "# Overview"


# ─── T4: promote is orthogonal to status ─────────────────────────────────────


@pytest.mark.asyncio
async def test_t4_promote_sets_orthogonal_fk(client, pool):
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/comments/{comment_id}/promote",
        headers=auth_headers(USER_A_ID),
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()
    # Status unchanged — promote is orthogonal
    assert body["status"] == "open"
    assert body["promoted_to_directive_id"] is not None

    # A new guideline row was created
    guideline_count = await pool.fetchval(
        "SELECT COUNT(*) FROM kb_directives WHERE kb_id = $1::uuid AND kind = 'guideline' AND archived_at IS NULL",
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
        f"/v1/knowledge-bases/{KB_A_ID}/pages/{ORPHAN_KEY}/comments",
        headers=auth_headers(USER_A_ID),
    )
    assert resp.status_code == 200
    assert resp.json() == []

    row = await pool.fetchrow(
        "SELECT status, system_note FROM kb_directives WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "archived"
    assert row["system_note"] == "orphaned"


# ─── T6: compile failure marks failed, retry resolves ────────────────────────


@pytest.mark.asyncio
async def test_t6_compile_failure_marks_failed_and_retries(client, pool, monkeypatch):
    await _seed_active_release(pool, RELEASE_ID, page_key=PAGE_KEY)
    comment_id = await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    call_count = 0

    async def fake_invoke(prompt, target):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated provider failure")
        return {
            "stop_reason": "stop",
            "request_id": "req-retry",
            "text_excerpt": "AUTOMATION SUMMARY\n- Fixed",
        }

    monkeypatch.setattr("services.periodic_compile._invoke_provider", fake_invoke)

    # First compile: fails
    resp = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "failed"

    row = await pool.fetchrow(
        "SELECT status, failure_reason FROM kb_directives WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "failed"
    assert row["failure_reason"] == "ProviderError"  # RuntimeError is scrubbed

    # Reset schedule so the KB is due again for the retry
    await pool.execute(
        "UPDATE knowledge_base_settings SET next_run_at = now() WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )

    # Second compile: succeeds — failed comment is in snapshot (status IN ('open','failed'))
    resp = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "succeeded"

    row = await pool.fetchrow(
        "SELECT status, compiled_run_id, compiled_at FROM kb_directives WHERE id = $1::uuid",
        comment_id,
    )
    assert row["status"] == "resolved"
    assert row["compiled_run_id"] is not None
    assert row["compiled_at"] is not None


# ─── failure_reason scrubbing (unit) ─────────────────────────────────────────


def test_failure_reason_scrubs_unknown_exceptions_to_ProviderError():
    from services.periodic_compile import _safe_failure_reason

    assert _safe_failure_reason(RuntimeError("boom")) == "ProviderError"
    assert _safe_failure_reason(ValueError("sensitive data")) == "ProviderError"
    assert _safe_failure_reason(Exception("unknown")) == "ProviderError"


def test_failure_reason_passes_through_known_exception_names():
    from services.periodic_compile import _safe_failure_reason

    class ProviderTimeout(Exception):
        pass

    class ProviderRateLimited(Exception):
        pass

    class TimeoutError(Exception):  # noqa: A001
        pass

    assert _safe_failure_reason(ProviderTimeout()) == "ProviderTimeout"
    assert _safe_failure_reason(ProviderRateLimited()) == "ProviderRateLimited"
    assert _safe_failure_reason(TimeoutError()) == "TimeoutError"


# ─── Skip logic: silent advance when no new work ─────────────────────────────


@pytest.mark.asyncio
async def test_compile_skipped_when_no_new_work(client, pool):
    # Push last_run_at into the future so the pre-flight query sees no new sources
    await pool.execute(
        "UPDATE knowledge_base_settings SET last_run_at = now() + interval '1 hour' "
        "WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )
    # No comments seeded

    runs_before = await pool.fetchval("SELECT COUNT(*) FROM compile_runs")

    resp = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "skipped"

    runs_after = await pool.fetchval("SELECT COUNT(*) FROM compile_runs")
    assert runs_after == runs_before  # silent advance — no compile_runs row

    next_run = await pool.fetchval(
        "SELECT next_run_at FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )
    assert next_run > datetime.now(timezone.utc)


# ─── Skip logic: Amendment 3 — comment not starved ───────────────────────────


@pytest.mark.asyncio
async def test_comment_not_starved_when_inserted_after_preflight(client, pool):
    # No new sources in pre-flight (last_run_at beyond document updated_at)
    await pool.execute(
        "UPDATE knowledge_base_settings SET last_run_at = now() + interval '1 hour' "
        "WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )
    # Also make the source non-pending inside run_target (checkpoint matches)
    await pool.execute(
        "INSERT INTO compiled_source_checkpoints (knowledge_base_id, document_id, compiled_version, compiled_at) "
        "VALUES ($1::uuid, $2::uuid, 1, now())",
        KB_A_ID,
        DOC_ID,
    )

    # Seed open comment — pre-flight sees new_cmt IS NOT NULL → has_work=True
    await _seed_comment(pool, page_key=PAGE_KEY, status="open")

    runs_before = await pool.fetchval("SELECT COUNT(*) FROM compile_runs")

    resp = await client.post(
        "/internal/compile-due",
        headers={"x-llmwiki-automation-secret": "test-automation-secret"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["status"] == "skipped"

    runs_after = await pool.fetchval("SELECT COUNT(*) FROM compile_runs")
    assert runs_after == runs_before + 1  # Amendment 3: visible skipped run, not silent

    skipped_run = await pool.fetchrow(
        "SELECT status FROM compile_runs ORDER BY started_at DESC LIMIT 1"
    )
    assert skipped_run["status"] == "skipped"


# ─── Pull-forward: new comment advances next_run_at ──────────────────────────


@pytest.mark.asyncio
async def test_new_comment_pulls_next_run_at_forward(client, pool):
    await _seed_active_release(pool, RELEASE_ID, page_key=PAGE_KEY)

    # Set next_run_at far in the future
    await pool.execute(
        "UPDATE knowledge_base_settings SET next_run_at = now() + interval '2 hours' "
        "WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )

    resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/pages/{PAGE_KEY}/comments",
        headers=auth_headers(USER_A_ID),
        json={"body": "Add more detail to the intro"},
    )
    assert resp.status_code == 201

    row = await pool.fetchrow(
        "SELECT next_run_at FROM knowledge_base_settings WHERE knowledge_base_id = $1::uuid",
        KB_A_ID,
    )
    deadline = datetime.now(timezone.utc) + timedelta(seconds=31)
    assert row["next_run_at"] <= deadline
