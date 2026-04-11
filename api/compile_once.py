import asyncio
import json
import sys

import asyncpg

from config import settings
from services.periodic_compile import load_target_from_settings, run_target


async def main() -> int:
    if not settings.LLMWIKI_COMPILE_KB:
        raise RuntimeError("LLMWIKI_COMPILE_KB is required")
    pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=5)
    try:
        target = await load_target_from_settings(pool, settings.LLMWIKI_COMPILE_KB)
        results = [await run_target(pool, target)]
    finally:
        await pool.close()

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except Exception as exc:
        print(f"compile_once failed: {exc}", file=sys.stderr)
        raise
