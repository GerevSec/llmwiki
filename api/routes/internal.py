from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from config import settings
from services.periodic_compile import run_due_schedules

router = APIRouter(prefix="/internal", tags=["internal"])


@router.post("/compile-due")
async def compile_due_knowledge_bases(
    request: Request,
    x_llmwiki_automation_secret: str | None = Header(default=None),
) -> list[dict[str, Any]]:
    if not settings.LLMWIKI_AUTOMATION_SECRET:
        raise HTTPException(status_code=503, detail="Automation secret is not configured")
    if x_llmwiki_automation_secret != settings.LLMWIKI_AUTOMATION_SECRET:
        raise HTTPException(status_code=401, detail="Invalid automation secret")

    pool = request.app.state.pool
    return await run_due_schedules(pool)
