"""Pre-deploy migration runner.

Applies any *.sql files in db_migrations/ that haven't been recorded in
the _repo_migrations_applied tracker table. Designed to run as Railway's
preDeployCommand before the API starts serving traffic.

Bootstrap behavior: on the very first run after the tracker table is
created, if the database already shows evidence of the latest known
migration (the kb_directives table from 010_kb_directives_unify.sql),
all currently-known migration filenames are recorded as already-applied.
This handles the one-time transition from manual migration application
(Supabase dashboard / MCP) to this runner without re-applying everything.

Idempotent: safe to run on every deploy. Each migration runs in its own
transaction; failure aborts the deploy.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import asyncpg


TRACKER_TABLE = "_repo_migrations_applied"
DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "db_migrations"
BOOTSTRAP_PROBE_TABLE = "kb_directives"


def discover_migrations(directory: Path) -> list[Path]:
    if not directory.exists() or not directory.is_dir():
        return []
    return sorted(p for p in directory.glob("*.sql") if p.is_file())


async def _ensure_tracker(conn: asyncpg.Connection) -> None:
    await conn.execute(
        f"CREATE TABLE IF NOT EXISTS {TRACKER_TABLE} ("
        "  filename TEXT PRIMARY KEY,"
        "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()"
        ")"
    )


async def _table_exists(conn: asyncpg.Connection, name: str) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=$1)",
            name,
        )
    )


async def _applied_set(conn: asyncpg.Connection) -> set[str]:
    rows = await conn.fetch(f"SELECT filename FROM {TRACKER_TABLE}")
    return {r["filename"] for r in rows}


async def _record(conn: asyncpg.Connection, filename: str) -> None:
    await conn.execute(
        f"INSERT INTO {TRACKER_TABLE} (filename) VALUES ($1) ON CONFLICT DO NOTHING",
        filename,
    )


async def run(database_url: str, migrations_dir: Path) -> int:
    files = discover_migrations(migrations_dir)
    if not files:
        print(f"[migrate] no migrations found in {migrations_dir}")
        return 0

    conn = await asyncpg.connect(database_url, ssl="require")
    try:
        await _ensure_tracker(conn)

        applied = await _applied_set(conn)

        # Bootstrap: tracker just created (empty) but database already has
        # evidence of the latest known migration. Record all current files
        # as applied without re-running them.
        if not applied and await _table_exists(conn, BOOTSTRAP_PROBE_TABLE):
            print(
                f"[migrate] bootstrap: {BOOTSTRAP_PROBE_TABLE} already exists; "
                f"recording {len(files)} known migrations as applied"
            )
            for f in files:
                await _record(conn, f.name)
            return 0

        applied_count = 0
        for f in files:
            if f.name in applied:
                continue
            print(f"[migrate] applying {f.name}")
            sql = f.read_text()
            async with conn.transaction():
                await conn.execute(sql)
                await _record(conn, f.name)
            applied_count += 1
            print(f"[migrate] ✓ {f.name}")

        if applied_count == 0:
            print(f"[migrate] up to date ({len(files)} known)")
        else:
            print(f"[migrate] applied {applied_count} new migration(s)")
        return applied_count
    finally:
        await conn.close()


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("[migrate] ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    migrations_dir_env = os.environ.get("MIGRATIONS_DIR")
    migrations_dir = Path(migrations_dir_env) if migrations_dir_env else DEFAULT_MIGRATIONS_DIR

    asyncio.run(run(database_url, migrations_dir))


if __name__ == "__main__":
    main()
