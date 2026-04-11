import pytest

from services.kb_access import resolve_kb_access


class FakePool:
    def __init__(self, row):
        self.row = row
        self.calls = []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return self.row


@pytest.mark.asyncio
async def test_resolve_kb_access_uses_uuid_lookup_for_uuid_identifiers():
    pool = FakePool(
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "slug": "test-kb",
            "name": "Test KB",
            "owner_user_id": "user-1",
            "role": "owner",
        }
    )

    access = await resolve_kb_access(pool, "user-1", "11111111-1111-1111-1111-111111111111")

    assert access is not None
    assert "kb.id::text" in pool.calls[0][0]


@pytest.mark.asyncio
async def test_resolve_kb_access_uses_slug_lookup_for_non_uuid_identifiers():
    pool = FakePool(
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "slug": "test-kb",
            "name": "Test KB",
            "owner_user_id": "user-1",
            "role": "editor",
        }
    )

    access = await resolve_kb_access(pool, "user-1", "test-kb")

    assert access is not None
    assert "kb.slug" in pool.calls[0][0]
