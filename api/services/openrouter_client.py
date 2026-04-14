from __future__ import annotations

from typing import Any

import httpx

from config import settings

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_DEFAULT_PLUGINS: tuple[dict[str, Any], ...] = ({"id": "context-compression"},)


def openrouter_headers(*, api_key: str, title: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": settings.APP_URL,
        "X-OpenRouter-Title": title,
    }


def openrouter_payload(**payload: Any) -> dict[str, Any]:
    body = dict(payload)
    body.setdefault("plugins", [dict(plugin) for plugin in OPENROUTER_DEFAULT_PLUGINS])
    return body


async def post_openrouter_chat_completion(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    title: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    response = await client.post(
        OPENROUTER_API_URL,
        headers=openrouter_headers(api_key=api_key, title=title),
        json=openrouter_payload(**payload),
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = _extract_openrouter_error_detail(exc.response)
        if detail:
            raise RuntimeError(f"OpenRouter request failed ({exc.response.status_code}): {detail}") from exc
        raise
    return response.json()


def _extract_openrouter_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        text = response.text.strip()
        return text[:500] if text else ""
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("metadata") or error.get("code")
            if isinstance(message, str):
                return message[:500]
        message = payload.get("message")
        if isinstance(message, str):
            return message[:500]
    return ""
