import pytest

from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, KB_B_ID, USER_A_ID, USER_B_ID, USER_B_EMAIL


@pytest.fixture(autouse=True)
async def seed_collaboration_data(pool):
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
        "INSERT INTO users (id, email, display_name) VALUES ($1, $2, 'Bob')",
        USER_B_ID,
        USER_B_EMAIL,
    )
    await pool.execute(
        "INSERT INTO knowledge_bases (id, user_id, name, slug) VALUES ($1, $2, 'Alice KB', 'alice-kb')",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_bases (id, user_id, name, slug) VALUES ($1, $2, 'Bob KB', 'bob-kb')",
        KB_B_ID,
        USER_B_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner')",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner')",
        KB_B_ID,
        USER_B_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_settings (knowledge_base_id, updated_by) VALUES ($1, $2)",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_settings (knowledge_base_id, updated_by) VALUES ($1, $2)",
        KB_B_ID,
        USER_B_ID,
    )

    yield


@pytest.mark.asyncio
async def test_owner_can_invite_and_accept_collaborator(client):
    invite_resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/invites",
        headers=auth_headers(USER_A_ID),
        json={"email": USER_B_EMAIL, "role": "editor"},
    )
    assert invite_resp.status_code == 201
    token = invite_resp.json()["token"]

    accept_resp = await client.post(
        "/v1/knowledge-bases/invites/accept",
        headers=auth_headers(USER_B_ID),
        json={"token": token},
    )
    assert accept_resp.status_code == 200
    assert accept_resp.json()["slug"] == "alice-kb"

    member_list = await client.get(
        f"/v1/knowledge-bases/{KB_A_ID}/members",
        headers=auth_headers(USER_A_ID),
    )
    assert member_list.status_code == 200
    assert any(member["user_id"] == USER_B_ID for member in member_list.json())


@pytest.mark.asyncio
async def test_pending_invites_are_visible_to_matching_email_only(client):
    invite_resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/invites",
        headers=auth_headers(USER_A_ID),
        json={"email": USER_B_EMAIL, "role": "viewer"},
    )
    assert invite_resp.status_code == 201

    pending_for_bob = await client.get(
        "/v1/knowledge-bases/invites/pending",
        headers=auth_headers(USER_B_ID),
    )
    assert pending_for_bob.status_code == 200
    assert len(pending_for_bob.json()) == 1

    pending_for_alice = await client.get(
        "/v1/knowledge-bases/invites/pending",
        headers=auth_headers(USER_A_ID),
    )
    assert pending_for_alice.status_code == 200
    assert pending_for_alice.json() == []


@pytest.mark.asyncio
async def test_invite_accept_fails_for_wrong_email(client):
    invite_resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/invites",
        headers=auth_headers(USER_A_ID),
        json={"email": "charlie@test.com", "role": "viewer"},
    )
    assert invite_resp.status_code == 201
    token = invite_resp.json()["token"]

    accept_resp = await client.post(
        "/v1/knowledge-bases/invites/accept",
        headers=auth_headers(USER_B_ID),
        json={"token": token},
    )
    assert accept_resp.status_code == 403


@pytest.mark.asyncio
async def test_collaborator_sees_shared_kb_in_list(client, pool):
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'viewer')",
        KB_A_ID,
        USER_B_ID,
    )

    response = await client.get("/v1/knowledge-bases", headers=auth_headers(USER_B_ID))
    assert response.status_code == 200
    slugs = [kb["slug"] for kb in response.json()]
    assert "alice-kb" in slugs
    assert "bob-kb" in slugs


@pytest.mark.asyncio
async def test_non_admin_cannot_update_compile_schedule(client, pool):
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'viewer')",
        KB_A_ID,
        USER_B_ID,
    )

    response = await client.put(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_B_ID),
        json={
            "enabled": True,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "interval_minutes": 60,
            "max_sources": 2,
            "prompt": "",
            "provider_secret": "secret",
            "max_tool_rounds": 50,
            "max_tokens": 50000,
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_owner_can_update_compile_schedule(client):
    response = await client.put(
        f"/v1/knowledge-bases/{KB_A_ID}/compile-schedule",
        headers=auth_headers(USER_A_ID),
        json={
            "enabled": True,
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4.6",
            "interval_minutes": 60,
            "max_sources": 2,
            "prompt": "Test prompt",
            "provider_secret": "secret",
            "max_tool_rounds": 50,
            "max_tokens": 50000,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["provider"] == "openrouter"
    assert body["has_provider_secret"] is True


@pytest.mark.asyncio
async def test_cannot_invite_existing_member(client, pool):
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'viewer')",
        KB_A_ID,
        USER_B_ID,
    )

    response = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/invites",
        headers=auth_headers(USER_A_ID),
        json={"email": USER_B_EMAIL, "role": "viewer"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "That user already has access to this knowledge base"


@pytest.mark.asyncio
async def test_unregistered_user_can_accept_after_signing_up(client, pool):
    invite_resp = await client.post(
        f"/v1/knowledge-bases/{KB_A_ID}/invites",
        headers=auth_headers(USER_A_ID),
        json={"email": "charlie@test.com", "role": "viewer"},
    )
    assert invite_resp.status_code == 201

    await pool.execute(
        "INSERT INTO users (id, email, display_name) VALUES ($1, 'charlie@test.com', 'Charlie')",
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
    )

    pending_resp = await client.get(
        "/v1/knowledge-bases/invites/pending",
        headers=auth_headers("cccccccc-cccc-cccc-cccc-cccccccccccc"),
    )
    assert pending_resp.status_code == 200
    assert len(pending_resp.json()) == 1

    accept_resp = await client.post(
        "/v1/knowledge-bases/invites/accept",
        headers=auth_headers("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        json={"invite_id": pending_resp.json()[0]["id"]},
    )
    assert accept_resp.status_code == 200
    assert accept_resp.json()["slug"] == "alice-kb"
