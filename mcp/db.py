"""Async connection pool for MCP tools."""

import asyncpg

from config import settings

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            settings.DATABASE_URL, min_size=1, max_size=5, command_timeout=15,
        )
    return _pool


async def scoped_query(user_id: str, sql: str, *args) -> list[dict]:
    """Execute a query scoped to a user via RLS."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL ROLE authenticated")
            claims = '{"sub":"' + user_id.replace('"', '') + '"}'
            await conn.execute(f"SET LOCAL request.jwt.claims = '{claims}'")
            rows = await conn.fetch(sql, *args)
            return [dict(r) for r in rows]


async def scoped_queryrow(user_id: str, sql: str, *args) -> dict | None:
    rows = await scoped_query(user_id, sql, *args)
    return rows[0] if rows else None


async def scoped_execute(user_id: str, sql: str, *args) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL ROLE authenticated")
            claims = '{"sub":"' + user_id.replace('"', '') + '"}'
            await conn.execute(f"SET LOCAL request.jwt.claims = '{claims}'")
            return await conn.execute(sql, *args)
