"""Integration tests for MCP tool SQL contracts and parameter shapes.

MCP tools cannot be imported directly (naming conflict with the `mcp` package
and missing `aioboto3`), so we test the SQL contracts by running the same
queries against the integration pool and use Python AST inspection for the
search tool's parameter shape.
"""

import ast
from pathlib import Path

import pytest

from tests.integration.isolation.conftest import KB_A_ID, KB_B_ID, USER_A_ID, USER_B_ID

# ─── Seed ─────────────────────────────────────────────────────────────────────

NO_MEMBER_USER_ID = "00000000-0000-0000-0000-000000000099"


@pytest.fixture(autouse=True)
async def mcp_seed(pool):
    """Clean slate + two users with separate KBs."""
    await pool.execute("DELETE FROM kb_directives")
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
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner')",
        KB_A_ID,
        USER_A_ID,
    )
    await pool.execute(
        "INSERT INTO knowledge_base_memberships (knowledge_base_id, user_id, role) VALUES ($1, $2, 'owner')",
        KB_B_ID,
        USER_B_ID,
    )
    yield


# ─── SQL copied from mcp/tools/list_knowledge_bases.py ───────────────────────

_LIST_KBS_SQL = (
    "SELECT kb.slug AS kb_slug, kb.id::text AS kb_id, kb.name, m.role "
    "FROM knowledge_bases kb "
    "JOIN knowledge_base_memberships m ON m.knowledge_base_id = kb.id AND m.user_id = $1 "
    "ORDER BY kb.name"
)

# ─── SQL copied from mcp/tools/get_kb_guidelines.py ─────────────────────────

_GET_GUIDELINES_SQL = (
    "SELECT body FROM kb_directives "
    "WHERE kb_id = $1 AND kind = 'guideline' AND is_active "
    "ORDER BY position, created_at"
)


# ─── list_knowledge_bases ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_knowledge_bases_returns_only_user_scoped(pool):
    rows = await pool.fetch(_LIST_KBS_SQL, USER_A_ID)

    assert len(rows) == 1
    assert rows[0]["kb_slug"] == "alice-kb"
    assert rows[0]["role"] == "owner"


@pytest.mark.asyncio
async def test_list_knowledge_bases_excludes_other_users_kb(pool):
    # USER_A should not see Bob's KB
    rows_a = await pool.fetch(_LIST_KBS_SQL, USER_A_ID)
    slugs_a = {r["kb_slug"] for r in rows_a}
    assert "bob-kb" not in slugs_a

    # USER_B should not see Alice's KB
    rows_b = await pool.fetch(_LIST_KBS_SQL, USER_B_ID)
    slugs_b = {r["kb_slug"] for r in rows_b}
    assert "alice-kb" not in slugs_b


@pytest.mark.asyncio
async def test_list_knowledge_bases_empty_when_no_memberships(pool):
    rows = await pool.fetch(_LIST_KBS_SQL, NO_MEMBER_USER_ID)
    assert rows == []


# ─── get_kb_guidelines ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_kb_guidelines_returns_active_guidelines(pool):
    await pool.execute(
        "INSERT INTO kb_directives (kb_id, kind, body, position, is_active, author_id) "
        "VALUES ($1::uuid, 'guideline', 'Always cite sources', 1, true, $2::uuid), "
        "       ($1::uuid, 'guideline', 'Use plain language', 2, true, $2::uuid)",
        KB_A_ID,
        USER_A_ID,
    )

    rows = await pool.fetch(_GET_GUIDELINES_SQL, KB_A_ID)

    assert len(rows) == 2
    assert rows[0]["body"] == "Always cite sources"
    assert rows[1]["body"] == "Use plain language"


@pytest.mark.asyncio
async def test_get_kb_guidelines_excludes_inactive(pool):
    await pool.execute(
        "INSERT INTO kb_directives (kb_id, kind, body, position, is_active, author_id) "
        "VALUES ($1::uuid, 'guideline', 'Active rule', 1, true, $2::uuid), "
        "       ($1::uuid, 'guideline', 'Inactive rule', 2, false, $2::uuid)",
        KB_A_ID,
        USER_A_ID,
    )

    rows = await pool.fetch(_GET_GUIDELINES_SQL, KB_A_ID)

    assert len(rows) == 1
    assert rows[0]["body"] == "Active rule"


@pytest.mark.asyncio
async def test_get_kb_guidelines_empty_when_none(pool):
    rows = await pool.fetch(_GET_GUIDELINES_SQL, KB_A_ID)
    assert rows == []


# ─── search: kb_slug is a required parameter ─────────────────────────────────


def test_search_kb_slug_is_required_parameter():
    """Parse search.py AST and verify kb_slug has no default (i.e., is required)."""
    search_path = Path(__file__).parent.parent.parent / "mcp" / "tools" / "search.py"
    tree = ast.parse(search_path.read_text())

    search_fn: ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "search":
            search_fn = node
            break

    assert search_fn is not None, "search() function not found in mcp/tools/search.py"

    args = search_fn.args
    # defaults only cover the trailing N positional args; params without defaults are required
    n_defaults = len(args.defaults)
    n_args = len(args.args)
    required_args = [arg.arg for arg in args.args[: n_args - n_defaults]]

    # kb_slug must be required (no default)
    assert "kb_slug" in required_args, (
        f"kb_slug should be a required parameter but has a default. "
        f"Required params found: {required_args}"
    )
