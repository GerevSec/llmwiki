"""Supabase JWT verification for MCP connections."""

import logging

import jwt as pyjwt
from jwt import PyJWKClient
from mcp.server.auth.provider import AccessToken, TokenVerifier

from config import settings

logger = logging.getLogger(__name__)

_jwks_client: PyJWKClient | None = None


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        jwks_url = f"{settings.SUPABASE_URL}/auth/v1/.well-known/jwks.json"
        _jwks_client = PyJWKClient(jwks_url)
    return _jwks_client


class SupabaseTokenVerifier(TokenVerifier):

    async def verify_token(self, token: str) -> AccessToken | None:
        payload = self._decode_jwt(token)
        if payload is None:
            return None

        sub = payload.get("sub", "")
        if not sub:
            logger.warning("JWT has no sub claim")
            return None

        logger.info("MCP auth: %s", sub)
        return AccessToken(token=token, client_id=sub, scopes=[])

    def _decode_jwt(self, token: str) -> dict | None:
        if settings.SUPABASE_URL:
            try:
                signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
                payload = pyjwt.decode(
                    token, signing_key.key,
                    algorithms=["ES256", "RS256"],
                    options={"verify_aud": False},
                )
                return payload
            except Exception as e:
                logger.debug("JWKS decode failed: %s", e)

        if settings.SUPABASE_JWT_SECRET:
            try:
                payload = pyjwt.decode(
                    token,
                    settings.SUPABASE_JWT_SECRET,
                    algorithms=["HS256"],
                    options={"verify_aud": False},
                )
                return payload
            except pyjwt.PyJWTError as e:
                logger.debug("HS256 decode failed: %s", e)

        return None
