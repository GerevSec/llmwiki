import re
import secrets
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from deps import get_scoped_db
from scoped_db import ScopedDB

router = APIRouter(prefix="/v1/knowledge-bases", tags=["knowledge-bases"])


class CreateKnowledgeBase(BaseModel):
    name: str


class UpdateKnowledgeBase(BaseModel):
    name: str


class KnowledgeBaseOut(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    slug: str
    created_at: datetime
    updated_at: datetime


def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug).strip("-")
    return slug or "kb"


async def _unique_slug(db: ScopedDB, name: str) -> str:
    slug = _slugify(name)
    exists = await db.fetchval(
        "SELECT 1 FROM knowledge_bases WHERE slug = $1 AND user_id = auth.uid()",
        slug,
    )
    if exists:
        slug = f"{slug}-{secrets.token_hex(3)}"
    return slug


@router.get("", response_model=list[KnowledgeBaseOut])
async def list_knowledge_bases(db: Annotated[ScopedDB, Depends(get_scoped_db)]):
    rows = await db.fetch(
        "SELECT id, user_id, name, slug, created_at, updated_at "
        "FROM knowledge_bases ORDER BY created_at DESC"
    )
    return rows


@router.post("", response_model=KnowledgeBaseOut, status_code=201)
async def create_knowledge_base(
    body: CreateKnowledgeBase,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    slug = await _unique_slug(db, body.name)
    row = await db.fetchrow(
        "INSERT INTO knowledge_bases (user_id, name, slug) "
        "VALUES (auth.uid(), $1, $2) "
        "RETURNING id, user_id, name, slug, created_at, updated_at",
        body.name,
        slug,
    )
    return row


@router.get("/{kb_id}", response_model=KnowledgeBaseOut)
async def get_knowledge_base(
    kb_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(
        "SELECT id, user_id, name, slug, created_at, updated_at "
        "FROM knowledge_bases WHERE id = $1",
        kb_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    return row


@router.patch("/{kb_id}", response_model=KnowledgeBaseOut)
async def update_knowledge_base(
    kb_id: UUID,
    body: UpdateKnowledgeBase,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(
        "UPDATE knowledge_bases SET name = $1, updated_at = now() "
        "WHERE id = $2 "
        "RETURNING id, user_id, name, slug, created_at, updated_at",
        body.name,
        kb_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Knowledge base not found")
    return row


@router.delete("/{kb_id}", status_code=204)
async def delete_knowledge_base(
    kb_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    result = await db.execute(
        "DELETE FROM knowledge_bases WHERE id = $1",
        kb_id,
    )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Knowledge base not found")
