"""Supavault MCP Server — knowledge vault tools for Claude."""

import os

import uvicorn
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from pydantic import AnyHttpUrl
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from auth import SupabaseTokenVerifier
from config import settings
from tools import register

mcp = FastMCP(
    "Supavault",
    token_verifier=SupabaseTokenVerifier(),
    auth=AuthSettings(
        issuer_url=AnyHttpUrl(f"{settings.SUPABASE_URL}/auth/v1"),
        resource_server_url=AnyHttpUrl(settings.API_URL),
    ),
)

register(mcp)


async def health(request):
    return PlainTextResponse("OK")


app = mcp.streamable_http_app()
app.router.routes.insert(0, Route("/health", health))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
