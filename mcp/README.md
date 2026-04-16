# MCP Tools

MCP tools expose the LLM Wiki knowledge vault to AI clients.

## Available Tools

| Tool | Description |
|------|-------------|
| `guide` | Get started — explains the wiki structure and workflow |
| `list_knowledge_bases` | List KBs accessible to the authenticated user |
| `get_kb_guidelines` | Return active guidelines for a KB as markdown bullets |
| `search` | Browse or keyword-search within a KB |
| `read` | Read document content from a KB |
| `write` | Create or edit wiki pages/notes in a KB |
| `delete` | Delete documents or wiki pages from a KB |

## Recommended Workflow

1. Call `list_knowledge_bases` → get `kb_slug` values
2. Optionally call `get_kb_guidelines(kb_slug)` before writing
3. Call `search`, `read`, `write`, or `delete` with the chosen `kb_slug`

## Breaking Changes

### v2 — kb_slug required on all KB-scoped tools

The `knowledge_base` parameter on `search`, `read`, `write`, and `delete` has been
**renamed to `kb_slug`** and is now **required** with no default.

**Removed behaviour:** passing an empty string to `search` previously listed all
knowledge bases. This fallback has been removed. Use `list_knowledge_bases` instead.

Clients will see the updated tool schemas on the next MCP handshake.
