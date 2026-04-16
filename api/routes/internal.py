from dataclasses import replace
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request

from config import settings
from services.periodic_compile import (
    load_target_from_settings,
    run_due_schedules,
    run_target,
)
from services.wiki_streamlining import run_due_streamlining

router = APIRouter(prefix="/internal", tags=["internal"])


def _check_automation_secret(x_llmwiki_automation_secret: str | None) -> None:
    if not settings.LLMWIKI_AUTOMATION_SECRET:
        raise HTTPException(status_code=503, detail="Automation secret is not configured")
    if x_llmwiki_automation_secret != settings.LLMWIKI_AUTOMATION_SECRET:
        raise HTTPException(status_code=401, detail="Invalid automation secret")


@router.post("/compile-due")
async def compile_due_knowledge_bases(
    request: Request,
    x_llmwiki_automation_secret: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    _check_automation_secret(x_llmwiki_automation_secret)
    pool = request.app.state.pool
    return await run_due_schedules(pool)


@router.post("/streamline-due")
async def streamline_due_knowledge_bases(
    request: Request,
    x_llmwiki_automation_secret: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    _check_automation_secret(x_llmwiki_automation_secret)
    pool = request.app.state.pool
    return await run_due_streamlining(pool)


@router.post("/recompile-from-scratch")
async def recompile_knowledge_base_from_scratch_internal(
    request: Request,
    slug: str = Query(..., description="Knowledge base slug"),
    x_llmwiki_automation_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Recompile a wiki from scratch (reset_wiki=True, force_all_sources=True)."""
    _check_automation_secret(x_llmwiki_automation_secret)
    pool = request.app.state.pool
    try:
        target = await load_target_from_settings(pool, slug)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target = replace(target, force_all_sources=True, reset_wiki=True)
    try:
        return await run_target(pool, target)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
