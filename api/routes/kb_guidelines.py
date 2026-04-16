from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from config import settings
from deps import get_user_id
from services.kb_access import ADMIN_ROLES, READ_ROLES, require_kb_access
from services.kb_guidelines import (
    create_guideline,
    delete_guideline,
    list_guidelines,
    update_guideline,
)

router = APIRouter(prefix="/api/kb", tags=["kb-guidelines"])


def _check_feature_flag() -> None:
    if not settings.ENABLE_KB_GUIDELINES_COMMENTS:
        raise HTTPException(status_code=404, detail="Not found")


class GuidelineCreate(BaseModel):
    body: str
    position: int | None = None


class GuidelineUpdate(BaseModel):
    body: str | None = None
    position: int | None = None
    is_active: bool | None = None


@router.get("/{kb_id}/guidelines")
async def get_guidelines(
    kb_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), READ_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    return await list_guidelines(pool, str(kb_id))


@router.post("/{kb_id}/guidelines", status_code=201)
async def post_guideline(
    kb_id: UUID,
    body: GuidelineCreate,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    return await create_guideline(pool, str(kb_id), body.body, user_id, body.position)


@router.patch("/{kb_id}/guidelines/{guideline_id}")
async def patch_guideline(
    kb_id: UUID,
    guideline_id: UUID,
    body: GuidelineUpdate,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    updated = await update_guideline(
        pool,
        str(guideline_id),
        body=body.body,
        position=body.position,
        is_active=body.is_active,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Guideline not found")
    return updated


@router.delete("/{kb_id}/guidelines/{guideline_id}", status_code=204)
async def delete_guideline_route(
    kb_id: UUID,
    guideline_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    deleted = await delete_guideline(pool, str(guideline_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="Guideline not found")
