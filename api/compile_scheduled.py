import asyncio
import json
import sys

import asyncpg

from services.periodic_compile import run_due_schedules


async def main() -> int:
    pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=5)
    try:
        results = await run_due_schedules(pool)
    finally:
        await pool.close()

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except Exception as exc:
        print(f"compile_scheduled failed: {exc}", file=sys.stderr)
        raise
