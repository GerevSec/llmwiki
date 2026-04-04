from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from deps import get_scoped_db
from scoped_db import ScopedDB

router = APIRouter(tags=["documents"])


class CreateNote(BaseModel):
    filename: str
    path: str = "/"
    content: str = ""


class UpdateContent(BaseModel):
    content: str


class UpdateMetadata(BaseModel):
    filename: str | None = None
    path: str | None = None
    title: str | None = None
    tags: list[str] | None = None


class DocumentOut(BaseModel):
    id: UUID
    knowledge_base_id: UUID
    user_id: UUID
    filename: str
    path: str
    title: str | None
    file_type: str
    status: str
    tags: list[str]
    version: int
    archived: bool
    created_at: datetime
    updated_at: datetime


class DocumentContent(BaseModel):
    id: UUID
    content: str | None
    version: int


@router.get("/v1/knowledge-bases/{kb_id}/documents", response_model=list[DocumentOut])
async def list_documents(
    kb_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
    path: str | None = Query(None),
):
    if path:
        rows = await db.fetch(
            "SELECT id, knowledge_base_id, user_id, filename, path, title, "
            "file_type, status, tags, version, archived, created_at, updated_at "
            "FROM documents WHERE knowledge_base_id = $1 AND archived = false AND path = $2 "
            "ORDER BY filename",
            kb_id,
            path,
        )
    else:
        rows = await db.fetch(
            "SELECT id, knowledge_base_id, user_id, filename, path, title, "
            "file_type, status, tags, version, archived, created_at, updated_at "
            "FROM documents WHERE knowledge_base_id = $1 AND archived = false "
            "ORDER BY filename",
            kb_id,
        )
    return rows


@router.post("/v1/knowledge-bases/{kb_id}/documents/note", response_model=DocumentOut, status_code=201)
async def create_note(
    kb_id: UUID,
    body: CreateNote,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    kb = await db.fetchval("SELECT id FROM knowledge_bases WHERE id = $1", kb_id)
    if not kb:
        raise HTTPException(status_code=404, detail="Knowledge base not found")

    row = await db.fetchrow(
        "INSERT INTO documents (knowledge_base_id, user_id, filename, path, title, "
        "file_type, status, content, tags) "
        "VALUES ($1, auth.uid(), $2, $3, $4, 'md', 'ready', $5, '{}') "
        "RETURNING id, knowledge_base_id, user_id, filename, path, title, "
        "file_type, status, tags, version, archived, created_at, updated_at",
        kb_id,
        body.filename,
        body.path,
        body.filename,
        body.content,
    )
    return row


@router.get("/v1/documents/{doc_id}", response_model=DocumentOut)
async def get_document(
    doc_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(
        "SELECT id, knowledge_base_id, user_id, filename, path, title, "
        "file_type, status, tags, version, archived, created_at, updated_at "
        "FROM documents WHERE id = $1",
        doc_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@router.get("/v1/documents/{doc_id}/content", response_model=DocumentContent)
async def get_document_content(
    doc_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(
        "SELECT id, content, version FROM documents WHERE id = $1",
        doc_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@router.put("/v1/documents/{doc_id}/content", response_model=DocumentContent)
async def update_document_content(
    doc_id: UUID,
    body: UpdateContent,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    row = await db.fetchrow(
        "UPDATE documents SET content = $1, version = version + 1, updated_at = now() "
        "WHERE id = $2 "
        "RETURNING id, content, version",
        body.content,
        doc_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@router.patch("/v1/documents/{doc_id}", response_model=DocumentOut)
async def update_document_metadata(
    doc_id: UUID,
    body: UpdateMetadata,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    updates = []
    params = []
    idx = 1

    if body.filename is not None:
        updates.append(f"filename = ${idx}")
        params.append(body.filename)
        idx += 1
    if body.path is not None:
        updates.append(f"path = ${idx}")
        params.append(body.path)
        idx += 1
    if body.title is not None:
        updates.append(f"title = ${idx}")
        params.append(body.title)
        idx += 1
    if body.tags is not None:
        updates.append(f"tags = ${idx}")
        params.append(body.tags)
        idx += 1

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = now()")
    params.append(doc_id)

    sql = (
        f"UPDATE documents SET {', '.join(updates)} "
        f"WHERE id = ${idx} "
        f"RETURNING id, knowledge_base_id, user_id, filename, path, title, "
        f"file_type, status, tags, version, archived, created_at, updated_at"
    )
    row = await db.fetchrow(sql, *params)
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@router.delete("/v1/documents/{doc_id}", status_code=204)
async def delete_document(
    doc_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    result = await db.execute(
        "UPDATE documents SET archived = true, updated_at = now() WHERE id = $1",
        doc_id,
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Document not found")
