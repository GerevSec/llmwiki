from __future__ import annotations

import posixpath
import re

MARKDOWNISH_FILE_TYPES = {"md", "txt", "note"}

_MARKDOWN_LINK_OPEN_RE = re.compile(r"!\[[^\]]*\]\(|\[[^\]]*\]\(")
_PATH_SUFFIX_RE = re.compile(r"^([^?#]*)(\?[^#]*)?(#.*)?$")
_EXTERNAL_PREFIXES = (
    "http://",
    "https://",
    "mailto:",
    "tel:",
    "data:",
    "javascript:",
)


def build_document_location(path: str, filename: str) -> str:
    joined = posixpath.join(path or "/", filename)
    normalized = posixpath.normpath(joined)
    return normalized if normalized.startswith("/") else f"/{normalized}"


def rebase_relative_markdown_links(
    content: str,
    old_document_location: str,
    new_document_location: str,
) -> str:
    old_dir = posixpath.dirname(old_document_location)
    new_dir = posixpath.dirname(new_document_location)

    def rewrite(destination: str) -> str | None:
        parsed = _parse_destination(destination)
        if parsed is None:
            return None

        raw_target, trailing, wrapped = parsed
        href_path, href_suffix = _split_path_suffix(raw_target)
        if _should_skip_href(href_path) or href_path.startswith("/"):
            return None

        resolved = _resolve_href(old_dir, href_path)
        rebased_target = new_document_location if resolved == old_document_location else resolved
        rebased = _make_relative_href(new_dir, rebased_target, href_path)
        if wrapped:
            rebased = f"<{rebased}>"
        return rebased + href_suffix + trailing

    return _rewrite_markdown_destinations(content, rewrite)


def rewrite_markdown_links_to_target(
    content: str,
    current_document_location: str,
    old_target_location: str,
    new_target_location: str,
) -> str:
    current_dir = posixpath.dirname(current_document_location)

    def rewrite(destination: str) -> str | None:
        parsed = _parse_destination(destination)
        if parsed is None:
            return None

        raw_target, trailing, wrapped = parsed
        href_path, href_suffix = _split_path_suffix(raw_target)
        if _should_skip_href(href_path):
            return None

        resolved = _resolve_href(current_dir, href_path)
        if resolved != old_target_location:
            return None

        if href_path.startswith("/"):
            rewritten = new_target_location
        else:
            rewritten = _make_relative_href(current_dir, new_target_location, href_path)
        if wrapped:
            rewritten = f"<{rewritten}>"
        return rewritten + href_suffix + trailing

    return _rewrite_markdown_destinations(content, rewrite)


def _rewrite_markdown_destinations(content: str, rewriter) -> str:
    parts: list[str] = []
    cursor = 0

    for match in _MARKDOWN_LINK_OPEN_RE.finditer(content):
        destination_bounds = _find_destination_bounds(content, match.end())
        if destination_bounds is None:
            continue

        start, end = destination_bounds
        raw_destination = content[start:end]
        replacement = rewriter(raw_destination.strip())
        if replacement is None or replacement == raw_destination.strip():
            continue

        parts.append(content[cursor:start])
        parts.append(replacement)
        cursor = end

    if not parts:
        return content

    parts.append(content[cursor:])
    return "".join(parts)


def _parse_destination(raw_destination: str) -> tuple[str, str, bool] | None:
    destination = raw_destination.strip()
    if not destination:
        return None

    if destination.startswith("<"):
        end = destination.find(">")
        if end == -1:
            return None
        return destination[1:end], destination[end + 1 :], True

    parts = destination.split(maxsplit=1)
    if len(parts) == 1:
        return parts[0], "", False
    return parts[0], f" {parts[1]}", False


def _should_skip_href(href: str) -> bool:
    lowered = href.lower()
    return not href or href.startswith("#") or href.startswith("//") or lowered.startswith(_EXTERNAL_PREFIXES)


def _find_destination_bounds(content: str, start: int) -> tuple[int, int] | None:
    depth = 1
    in_angle = False
    escape = False
    idx = start

    while idx < len(content):
        char = content[idx]
        if escape:
            escape = False
            idx += 1
            continue

        if char == "\\":
            escape = True
            idx += 1
            continue

        if char == "<" and not in_angle:
            in_angle = True
        elif char == ">" and in_angle:
            in_angle = False
        elif not in_angle:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return start, idx

        idx += 1

    return None


def _split_path_suffix(href: str) -> tuple[str, str]:
    match = _PATH_SUFFIX_RE.match(href)
    if not match:
        return href, ""
    return match.group(1), f"{match.group(2) or ''}{match.group(3) or ''}"


def _resolve_href(current_dir: str, href: str) -> str:
    path_part, _suffix = _split_path_suffix(href)
    if path_part.startswith("/"):
        resolved = posixpath.normpath(path_part)
    else:
        resolved = posixpath.normpath(posixpath.join(current_dir, path_part))
    return resolved if resolved.startswith("/") else f"/{resolved}"


def _make_relative_href(current_dir: str, target_location: str, original_href: str) -> str:
    relative = posixpath.relpath(target_location, start=current_dir or "/")
    if relative == ".":
        relative = posixpath.basename(target_location)
    if original_href.startswith("./") and not relative.startswith("."):
        return f"./{relative}"
    return relative
