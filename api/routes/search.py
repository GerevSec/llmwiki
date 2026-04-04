from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from deps import get_scoped_db
from scoped_db import ScopedDB

router = APIRouter(prefix="/v1/knowledge-bases", tags=["search"])


class SearchRequest(BaseModel):
    query: str
    limit: int = 10


class SearchResult(BaseModel):
    results: list[dict]


@router.post("/{kb_id}/search", response_model=SearchResult)
async def search_knowledge_base(
    kb_id: UUID,
    body: SearchRequest,
    db: Annotated[ScopedDB, Depends(get_scoped_db)],
):
    return SearchResult(results=[])
