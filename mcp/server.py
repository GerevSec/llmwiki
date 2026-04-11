"""Supavault MCP Server — knowledge vault tools for Claude."""

import json
import logging
import os
import time
import urllib.request
from urllib.error import URLError
from urllib.parse import urlparse

import logfire
import sentry_sdk
import uvicorn

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyHttpUrl
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from auth import SupabaseTokenVerifier
from config import settings
from tools import register

logger = logging.getLogger(__name__)

if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        send_default_pii=True,
        traces_sample_rate=0.1,
        environment=settings.STAGE,
    )

if settings.LOGFIRE_TOKEN:
    logfire.configure(token=settings.LOGFIRE_TOKEN, service_name="supavault-mcp")
    logfire.instrument_asyncpg()

_mcp_host = urlparse(settings.MCP_URL).hostname or "localhost"


def _public_mcp_url() -> str:
    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("RAILWAY_STATIC_URL")
    if public_domain:
        return f"https://{public_domain.rstrip('/')}/mcp"
    return settings.MCP_URL


def _public_mcp_base_url() -> str:
    return _public_mcp_url().removesuffix("/mcp")


def _supabase_issuer_url() -> str:
    return f"{settings.SUPABASE_URL.rstrip('/')}/auth/v1"

mcp = FastMCP(
    "LLM Wiki",
    instructions=(
        "You are connected to an LLM Wiki workspace. The user has uploaded files, notes, "
        "and documents that you can read, search, edit, and organize. Your job is to work "
        "with these materials — answer questions, take notes, and compile structured wiki "
        "pages from the raw sources. Call the `guide` tool first to see available knowledge "
        "bases and learn the full workflow."
    ),
    token_verifier=SupabaseTokenVerifier(),
    auth=AuthSettings(
        issuer_url=AnyHttpUrl(_supabase_issuer_url()),
        resource_server_url=AnyHttpUrl(_public_mcp_url()),
    ),
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[_mcp_host],
    ),
)

register(mcp)


async def health(request):
    return PlainTextResponse("OK")


async def protected_resource_metadata(request):
    return JSONResponse(
        {
            "resource": _public_mcp_url(),
            "authorization_servers": [_public_mcp_base_url()],
            "bearer_methods_supported": ["header"],
        }
    )


_STATIC_AUTH_METADATA_FALLBACK = {
    "response_types_supported": ["code"],
    "grant_types_supported": ["authorization_code", "refresh_token"],
    "token_endpoint_auth_methods_supported": [
        "none",
        "client_secret_post",
        "client_secret_basic",
    ],
    "code_challenge_methods_supported": ["S256", "plain"],
}


def _fetch_supabase_auth_metadata() -> dict | None:
    issuer_url = _supabase_issuer_url()
    discovery_url = f"{issuer_url}/.well-known/oauth-authorization-server"
    try:
        with urllib.request.urlopen(discovery_url, timeout=3) as response:
            return json.loads(response.read())
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        logger.warning("Failed to fetch Supabase OAuth metadata from %s: %s", discovery_url, exc)
        return None


def _build_auth_metadata() -> dict:
    issuer_url = _supabase_issuer_url()
    static_fallback = {
        "issuer": issuer_url,
        "authorization_endpoint": f"{issuer_url}/oauth/authorize",
        "token_endpoint": f"{issuer_url}/oauth/token",
        "registration_endpoint": f"{issuer_url}/oauth/register",
        **_STATIC_AUTH_METADATA_FALLBACK,
    }
    upstream = _fetch_supabase_auth_metadata()
    if upstream is None:
        return static_fallback
    if "registration_endpoint" not in upstream:
        upstream["registration_endpoint"] = f"{issuer_url}/oauth/register"
    return upstream


_AUTH_METADATA_TTL_SECONDS = 60
_cached_auth_metadata: dict = _build_auth_metadata()
_cached_auth_metadata_at: float = time.monotonic()


async def authorization_server_metadata(request):
    global _cached_auth_metadata, _cached_auth_metadata_at
    if time.monotonic() - _cached_auth_metadata_at > _AUTH_METADATA_TTL_SECONDS:
        _cached_auth_metadata = _build_auth_metadata()
        _cached_auth_metadata_at = time.monotonic()
    return JSONResponse(_cached_auth_metadata)


app = mcp.streamable_http_app()
app.router.routes.insert(0, Route("/health", health))
app.router.routes.insert(0, Route("/.well-known/oauth-protected-resource", protected_resource_metadata))
app.router.routes.insert(0, Route("/.well-known/oauth-protected-resource/mcp", protected_resource_metadata))
app.router.routes.insert(0, Route("/.well-known/oauth-authorization-server", authorization_server_metadata))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
