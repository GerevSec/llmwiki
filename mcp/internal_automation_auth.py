from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time


TOKEN_PREFIX = "llmwiki_internal"


def verify_internal_mcp_token(token: str, secret: str) -> str | None:
    try:
        prefix, payload_b64, sig_b64 = token.split(".", 2)
    except ValueError:
        return None
    if prefix != TOKEN_PREFIX:
        return None

    expected_sig = hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).digest()
    actual_sig = _urlsafe_decode(sig_b64)
    if not hmac.compare_digest(expected_sig, actual_sig):
        return None

    payload = json.loads(_urlsafe_decode(payload_b64))
    if payload.get("exp", 0) < int(time.time()):
        return None
    sub = payload.get("sub")
    return str(sub) if sub else None


def _urlsafe_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
