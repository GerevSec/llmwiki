from tests.helpers.jwt import auth_headers
from tests.integration.isolation.conftest import KB_A_ID, KB_B_ID, USER_A_ID, USER_B_ID

MOVED_DOC_ID = "11111111-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ROOT_DOC_ID = "22222222-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NESTED_DOC_ID = "33333333-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
OTHER_KB_DOC_ID = "44444444-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


async def test_moving_document_rewrites_related_markdown_links(client, pool):
    await pool.execute("DELETE FROM document_chunks")
    await pool.execute("DELETE FROM document_pages")
    await pool.execute("DELETE FROM documents")
    await pool.execute("DELETE FROM api_keys")
    await pool.execute("DELETE FROM knowledge_bases")
    await pool.execute("DELETE FROM users")
    await pool.execute(
        "INSERT INTO users (id, email, display_name) VALUES ($1, 'alice@test.com', 'Alice')",
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO users (id, email, display_name) VALUES ($1, 'bob@test.com', 'Bob')",
        USER_B_ID,
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
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'moved.md', 'Moved', '/folder/', 'md', 'ready', $4, 1)",
        MOVED_DOC_ID,
        KB_A_ID,
        USER_A_ID,
        "[Sibling](./sibling.md)\n[Root](../overview.md)\n![Sheet](./sheet.csv#tab=1)",
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'index.md', 'Index', '/', 'md', 'ready', $4, 1)",
        ROOT_DOC_ID,
        KB_A_ID,
        USER_A_ID,
        "[Moved](folder/moved.md)\n![Moved Image](folder/moved.md#section)\n[Other](folder/other.md)",
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'reader.md', 'Reader', '/deep/', 'md', 'ready', $4, 1)",
        NESTED_DOC_ID,
        KB_A_ID,
        USER_A_ID,
        "[Moved](../folder/moved.md)\n[Mail](mailto:test@example.com)",
    )
    await pool.execute(
        "INSERT INTO documents (id, knowledge_base_id, user_id, filename, title, path, file_type, status, content, version) "
        "VALUES ($1, $2, $3, 'index.md', 'Index', '/', 'md', 'ready', $4, 1)",
        OTHER_KB_DOC_ID,
        KB_B_ID,
        USER_B_ID,
        "[Moved](folder/moved.md)",
    )

    resp = await client.patch(
        f"/v1/documents/{MOVED_DOC_ID}",
        headers=auth_headers(USER_A_ID),
        json={"path": "/archive/"},
    )

    assert resp.status_code == 200
    assert resp.json()["path"] == "/archive/"

    moved_row = await pool.fetchrow(
        "SELECT path, content, version FROM documents WHERE id = $1",
        MOVED_DOC_ID,
    )
    root_row = await pool.fetchrow(
        "SELECT content, version FROM documents WHERE id = $1",
        ROOT_DOC_ID,
    )
    nested_row = await pool.fetchrow(
        "SELECT content, version FROM documents WHERE id = $1",
        NESTED_DOC_ID,
    )
    other_kb_row = await pool.fetchrow(
        "SELECT content FROM documents WHERE id = $1",
        OTHER_KB_DOC_ID,
    )

    assert moved_row["path"] == "/archive/"
    assert "[Sibling](../folder/sibling.md)" in moved_row["content"]
    assert "[Root](../overview.md)" in moved_row["content"]
    assert "![Sheet](../folder/sheet.csv#tab=1)" in moved_row["content"]
    assert moved_row["version"] == 2

    assert "[Moved](archive/moved.md)" in root_row["content"]
    assert "![Moved Image](archive/moved.md#section)" in root_row["content"]
    assert "[Other](folder/other.md)" in root_row["content"]
    assert root_row["version"] == 2

    assert "[Moved](../archive/moved.md)" in nested_row["content"]
    assert "[Mail](mailto:test@example.com)" in nested_row["content"]
    assert nested_row["version"] == 2

    assert other_kb_row["content"] == "[Moved](folder/moved.md)"
