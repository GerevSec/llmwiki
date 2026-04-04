from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from routes.health import router as health_router
from routes.knowledge_bases import router as knowledge_bases_router
from routes.documents import router as documents_router
from routes.search import router as search_router
from routes.api_keys import router as api_keys_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.DATABASE_URL, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(title="Supavault API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.APP_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(knowledge_bases_router)
app.include_router(documents_router)
app.include_router(search_router)
app.include_router(api_keys_router)
