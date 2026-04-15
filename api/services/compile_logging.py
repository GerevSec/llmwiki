"""Structured logging helpers for compile / recompile / streamline flows.

Adds consistent, parseable INFO-level log lines so Railway log search can
filter by event name. All log lines use the prefix `llmwiki.compile` or
`llmwiki.streamline` so they can be grepped together. The helpers below
truncate bulky payloads and avoid emitting provider secrets: call sites are
responsible for never passing api_key-like fields into log_event.
"""

from __future__ import annotations

import json
import logging
from typing import Any

COMPILE_LOGGER_NAME = "llmwiki.compile"
STREAMLINE_LOGGER_NAME = "llmwiki.streamline"


def _ensure_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.level == logging.NOTSET or logger.level > logging.INFO:
        logger.setLevel(logging.INFO)
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    logger.propagate = True
    return logger


compile_logger = _ensure_logger(COMPILE_LOGGER_NAME)
streamline_logger = _ensure_logger(STREAMLINE_LOGGER_NAME)

DEFAULT_PREVIEW_CHARS = 240
SENSITIVE_KEYS = {"api_key", "provider_api_key", "authorization", "x-api-key", "secret"}


def _safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _safe(v) for k, v in value.items() if k.lower() not in SENSITIVE_KEYS}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return str(value)


def preview(text: Any, limit: int = DEFAULT_PREVIEW_CHARS) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        try:
            text = json.dumps(_safe(text), ensure_ascii=False, default=str)
        except Exception:
            text = str(text)
    text = text.replace("\n", " ").replace("\r", " ")
    if len(text) > limit:
        return text[:limit] + f"…(+{len(text) - limit} chars)"
    return text


def _format(event: str, fields: dict[str, Any]) -> str:
    parts = [f"event={event}"]
    for key, value in fields.items():
        if key.lower() in SENSITIVE_KEYS:
            continue
        if value is None or value == "":
            continue
        if isinstance(value, (dict, list, tuple)):
            value = preview(value)
        elif isinstance(value, str):
            if "\n" in value or " " in value or "=" in value:
                value = json.dumps(value, ensure_ascii=False)
        parts.append(f"{key}={value}")
    return " ".join(parts)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    logger.info(_format(event, fields))


def log_compile(event: str, **fields: Any) -> None:
    log_event(compile_logger, event, **fields)


def log_streamline(event: str, **fields: Any) -> None:
    log_event(streamline_logger, event, **fields)
