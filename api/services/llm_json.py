from __future__ import annotations

import json
import re
from typing import Any

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
_JSON_OPENERS = {"{": "}", "[": "]"}
_JSON_CLOSERS = {"}": "{", "]": "["}
_VALID_ESCAPES = {'"', "\\", "/", "b", "f", "n", "r", "t", "u"}


def loads_lenient_json(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return {}

    first_error: json.JSONDecodeError | None = None
    for candidate in _candidate_json_strings(stripped):
        for variant in (candidate, _repair_json_string(candidate)):
            try:
                return json.loads(variant)
            except json.JSONDecodeError as exc:
                if first_error is None:
                    first_error = exc
    if first_error is not None:
        raise first_error
    raise json.JSONDecodeError("No JSON object found", stripped, 0)


def _candidate_json_strings(text: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(candidate: str | None) -> None:
        if not candidate:
            return
        normalized = candidate.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    add(text)
    for match in _JSON_FENCE_RE.finditer(text):
        add(match.group(1))
    add(_extract_balanced_json(text))
    return candidates


def _extract_balanced_json(text: str) -> str | None:
    start = -1
    for idx, char in enumerate(text):
        if char in _JSON_OPENERS:
            start = idx
            break
    if start < 0:
        return None

    stack: list[str] = []
    in_string = False
    escape = False

    for idx in range(start, len(text)):
        char = text[idx]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char in _JSON_OPENERS:
            stack.append(char)
            continue
        if char in _JSON_CLOSERS:
            if not stack or stack[-1] != _JSON_CLOSERS[char]:
                return None
            stack.pop()
            if not stack:
                return text[start : idx + 1]
    return None


def _repair_json_string(text: str) -> str:
    chars: list[str] = []
    in_string = False
    escape = False

    for idx, char in enumerate(text):
        if not in_string:
            chars.append(char)
            if char == '"':
                in_string = True
                escape = False
            continue

        if escape:
            chars.append(char)
            escape = False
            continue

        if char == "\\":
            next_char = text[idx + 1] if idx + 1 < len(text) else ""
            if next_char in _VALID_ESCAPES:
                chars.append(char)
                escape = True
            else:
                chars.append("\\\\")
            continue

        if char == '"':
            next_index = idx + 1
            while next_index < len(text) and text[next_index] in " \t\r\n":
                next_index += 1
            next_char = text[next_index] if next_index < len(text) else ""
            if next_char in {",", "}", "]", ":", ""}:
                chars.append(char)
                in_string = False
            else:
                chars.append('\\"')
            continue

        if char == "\n":
            chars.append("\\n")
            continue
        if char == "\r":
            chars.append("\\r")
            continue
        if char == "\t":
            chars.append("\\t")
            continue

        chars.append(char)

    return "".join(chars)
