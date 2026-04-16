"""Verify each MCP tool registers with the correct ToolAnnotations.

We can't import the `mcp.tools.*` modules directly in this test suite
(the top-level `mcp/` package collides with the upstream `mcp` SDK and
those files pull in `aioboto3` via helpers). Instead we parse each tool
file with the ast module and inspect the `@mcp.tool(...)` decorator.

Claude.ai and other MCP clients use `readOnlyHint` / `destructiveHint`
to decide whether to auto-approve a tool call. Getting these wrong
re-introduces a permission prompt for every wiki read, so the test is
strict about each field.
"""

import ast
from pathlib import Path

import pytest

MCP_TOOLS_DIR = Path(__file__).resolve().parents[2] / "mcp" / "tools"

READ_ONLY_TOOLS = {
    "guide": "guide.py",
    "list_knowledge_bases": "list_knowledge_bases.py",
    "get_kb_guidelines": "get_kb_guidelines.py",
    "search": "search.py",
    "read": "read.py",
}
WRITE_TOOLS = {"write": "write.py"}
DESTRUCTIVE_TOOLS = {"delete": "delete.py"}


def _extract_tool_annotations(file_path: Path, tool_name: str) -> dict[str, bool]:
    """Return a dict of annotation name → boolean value for the @mcp.tool decorator
    whose name kwarg equals `tool_name` inside `file_path`."""
    tree = ast.parse(file_path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            # Locate name= kwarg
            name_kw = next(
                (kw for kw in dec.keywords if kw.arg == "name" and isinstance(kw.value, ast.Constant)),
                None,
            )
            if not name_kw or name_kw.value.value != tool_name:
                continue
            ann_kw = next(
                (kw for kw in dec.keywords if kw.arg == "annotations"),
                None,
            )
            assert ann_kw is not None, f"{tool_name} missing annotations= kwarg"
            assert isinstance(ann_kw.value, ast.Call), f"{tool_name} annotations must be a ToolAnnotations(...) call"
            assert isinstance(ann_kw.value.func, ast.Name) and ann_kw.value.func.id == "ToolAnnotations", (
                f"{tool_name} annotations= must be `ToolAnnotations(...)`"
            )
            return {
                kw.arg: kw.value.value
                for kw in ann_kw.value.keywords
                if isinstance(kw.value, ast.Constant)
            }
    pytest.fail(f"Did not find @mcp.tool(name={tool_name!r}) decorator in {file_path}")


@pytest.mark.parametrize("tool_name,file_name", list(READ_ONLY_TOOLS.items()))
def test_read_only_tools_are_readonly(tool_name: str, file_name: str) -> None:
    ann = _extract_tool_annotations(MCP_TOOLS_DIR / file_name, tool_name)
    assert ann.get("readOnlyHint") is True, f"{tool_name} should set readOnlyHint=True so Claude.ai auto-approves"
    assert ann.get("destructiveHint") is False, f"{tool_name} must set destructiveHint=False"


@pytest.mark.parametrize("tool_name,file_name", list(WRITE_TOOLS.items()))
def test_write_tool_is_not_readonly_and_not_destructive(tool_name: str, file_name: str) -> None:
    ann = _extract_tool_annotations(MCP_TOOLS_DIR / file_name, tool_name)
    assert ann.get("readOnlyHint") is False, f"{tool_name} must not be read-only"
    assert ann.get("destructiveHint") is False, f"{tool_name} is not destructive (only delete is)"


@pytest.mark.parametrize("tool_name,file_name", list(DESTRUCTIVE_TOOLS.items()))
def test_delete_tool_is_destructive(tool_name: str, file_name: str) -> None:
    ann = _extract_tool_annotations(MCP_TOOLS_DIR / file_name, tool_name)
    assert ann.get("readOnlyHint") is False, f"{tool_name} must not be read-only"
    assert ann.get("destructiveHint") is True, f"{tool_name} must set destructiveHint=True"


def test_all_tools_have_annotations() -> None:
    """Regression guard: if a new tool is added, it must carry annotations."""
    registered = {
        **READ_ONLY_TOOLS,
        **WRITE_TOOLS,
        **DESTRUCTIVE_TOOLS,
    }
    for tool_file in MCP_TOOLS_DIR.glob("*.py"):
        if tool_file.name in {"__init__.py", "helpers.py", "wiki_release.py"}:
            continue
        assert tool_file.name in registered.values(), (
            f"New MCP tool file {tool_file.name} is not covered by the annotation test — "
            f"add it to READ_ONLY_TOOLS / WRITE_TOOLS / DESTRUCTIVE_TOOLS."
        )
