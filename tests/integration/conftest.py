import os
from pathlib import Path

import asyncpg
import httpx
import pytest

from tests.helpers.jwt import seed_jwks_cache

DB_URL = os.environ["DATABASE_URL"]


@pytest.fixture(scope="session")
async def pool():
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=20)

    await pool.execute("DROP SCHEMA IF EXISTS public CASCADE")
    await pool.execute("CREATE SCHEMA public")

    schema_sql = (Path(__file__).parent.parent / "helpers" / "schema.sql").read_text()
    await pool.execute(schema_sql)
    for migration in [
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "002_periodic_compile.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "003_compile_schedules_and_providers.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "004_collaboration_and_kb_settings.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "005_compile_defaults_and_limits.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "006_wiki_direct_editing.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "007_wiki_streamlining.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "008_compile_telemetry_and_recompile.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "009_kb_guidelines_and_comments.sql",
        Path(__file__).parent.parent.parent / "supabase" / "migrations" / "010_kb_directives_unify.sql",
    ]:
        await pool.execute(migration.read_text())

    yield pool
    pool.terminate()


@pytest.fixture
async def client(pool):
    from main import app

    app.state.pool = pool
    app.state.s3_service = None
    app.state.ocr_service = None

    seed_jwks_cache()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
