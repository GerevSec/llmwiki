from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from config import settings
from deps import get_user_id
from services.kb_access import ADMIN_ROLES, READ_ROLES, require_kb_access
from services.wiki_comments import (
    IllegalTransitionError,
    create_comment,
    list_page_comments,
    promote_comment,
    transition_comment,
)

router = APIRouter(prefix="/api/kb", tags=["wiki-comments"])


def _check_feature_flag() -> None:
    if settings.KB_GUIDELINES_COMMENTS_DISABLED:
        raise HTTPException(status_code=404, detail="Not found")


class CommentCreate(BaseModel):
    body: str


class PromoteBody(BaseModel):
    body: str | None = None


@router.get("/{kb_id}/pages/{page_key}/comments")
async def get_page_comments(
    kb_id: UUID,
    page_key: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
    response: Response,
):
    # This endpoint performs a lazy archival UPDATE and must be served from primary DB only.
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), READ_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    response.headers["Cache-Control"] = "no-store"
    return await list_page_comments(pool, str(kb_id), str(page_key))


@router.post("/{kb_id}/pages/{page_key}/comments", status_code=201)
async def post_page_comment(
    kb_id: UUID,
    page_key: UUID,
    body: CommentCreate,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), READ_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    return await create_comment(pool, str(kb_id), str(page_key), body.body, user_id)



@router.post("/{kb_id}/comments/{comment_id}/archive")
async def archive_comment(
    kb_id: UUID,
    comment_id: UUID,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    try:
        result = await transition_comment(pool, str(comment_id), "archived", user_id)
    except IllegalTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Comment not found")
    return result


@router.post("/{kb_id}/comments/{comment_id}/promote")
async def promote_comment_route(
    kb_id: UUID,
    comment_id: UUID,
    body: PromoteBody,
    user_id: Annotated[str, Depends(get_user_id)],
    request: Request,
):
    _check_feature_flag()
    pool = request.app.state.pool
    try:
        await require_kb_access(pool, user_id, str(kb_id), ADMIN_ROLES)
    except PermissionError as exc:
        raise HTTPException(status_code=404, detail="Knowledge base not found") from exc
    try:
        result = await promote_comment(pool, str(comment_id), body.body, user_id)
    except IllegalTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Comment not found")
    return result
