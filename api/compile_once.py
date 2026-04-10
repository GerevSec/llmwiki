import asyncio
import json
import sys

import asyncpg

from config import settings
from services.periodic_compile import load_compile_targets, run_target


async def main() -> int:
    if not settings.ANTHROPIC_API_KEY and not settings.LLMWIKI_COMPILE_DRY_RUN:
        raise RuntimeError("ANTHROPIC_API_KEY is required unless LLMWIKI_COMPILE_DRY_RUN=true")
    if not settings.ANTHROPIC_MODEL:
        raise RuntimeError("ANTHROPIC_MODEL is required")

    targets = load_compile_targets()
    pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=1, max_size=5)
    try:
        results = []
        for target in targets:
            results.append(await run_target(pool, target))
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
