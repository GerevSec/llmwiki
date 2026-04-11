import pytest

from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, KB_B_ID, USER_A_EMAIL, USER_A_ID, USER_B_EMAIL, USER_B_ID


@pytest.fixture(autouse=True)
async def seed_management_data(pool):
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
        "INSERT INTO users (id, email, display_name) VALUES ($1, $2, 'Alice')",
        USER_A_ID,
        USER_A_EMAIL,
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


@pytest.mark.asyncio
async def test_owner_can_delete_knowledge_base(client, pool):
    response = await client.delete(
        f"/v1/knowledge-bases/{KB_A_ID}",
        headers=auth_headers(USER_A_ID),
    )

    assert response.status_code == 204
    exists = await pool.fetchval("SELECT 1 FROM knowledge_bases WHERE id = $1", KB_A_ID)
    assert exists is None


@pytest.mark.asyncio
async def test_admin_cannot_delete_knowledge_base(client, pool):
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'admin')",
        KB_A_ID,
        USER_B_ID,
    )

    response = await client.delete(
        f"/v1/knowledge-bases/{KB_A_ID}",
        headers=auth_headers(USER_B_ID),
    )

    assert response.status_code == 404
