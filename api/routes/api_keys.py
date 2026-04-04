import hashlib
import secrets
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from deps import get_scoped_db
from scoped_db import ScopedDB

router = APIRouter(prefix="/v1/api-keys", tags=["api-keys"])


class CreateAPIKey(BaseModel):
    name: str = "Default"


class APIKeyOut(BaseModel):
    id: UUID
    name: str | None
    key_prefix: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None


class APIKeyCreated(APIKeyOut):
    key: str


@router.get("", response_model=list[APIKeyOut])
async def list_api_keys(db: Annotated[ScopedDB, Depends(get_scoped_db)]):
    rows = await db.fetch(
        "SELECT id, name, key_prefix, created_at, last_used_at, revoked_at "
        "FROM api_keys WHERE revoked_at IS NULL ORDER BY created_at DESC"
    )
    return rows


@router.post("", response_model=APIKeyCreated, status_code=201)
async def create_api_key(
    body: CreateAPIKey,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    raw_key = "sv_" + secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    key_prefix = raw_key[:11]

    row = await db.fetchrow(
        "INSERT INTO api_keys (user_id, name, key_hash, key_prefix) "
        "VALUES (auth.uid(), $1, $2, $3) "
        "RETURNING id, name, key_prefix, created_at, last_used_at, revoked_at",
        body.name, key_hash, key_prefix,
    )
    return {**row, "key": raw_key}


@router.delete("/{key_id}", status_code=204)
async def revoke_api_key(
    key_id: UUID,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    result = await db.execute(
        "UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
        key_id,
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="API key not found")
