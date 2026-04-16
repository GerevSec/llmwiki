from mcp.server.fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations

from config import settings
from db import scoped_query
from .helpers import get_user_id

GUIDE_TEXT = """# LLM Wiki — How It Works

You are connected to an **LLM Wiki** — a personal knowledge workspace where you compile and maintain a structured wiki from raw source documents.

## Architecture

1. **Raw Sources** (path: `/`) — uploaded documents (PDFs, notes, images, spreadsheets). Source of truth. Read-only.
2. **Compiled Wiki** (path: `/wiki/`) — markdown pages YOU create and maintain. You own this layer.
3. **Tools** — `search`, `read`, `write`, `delete` — your interface to both layers.

## Wiki Structure

Every wiki follows this structure. These categories are not suggestions — they are the backbone of the wiki.

### Overview (`/wiki/overview.md`) — THE HUB PAGE
Always exists. This is the front page of the wiki. It must contain:
- A summary of what this wiki covers and its scope
- **Source count** and page count (update on every ingest)
- **Key Findings** — the most important insights across all sources
- **Recent Updates** — last 5-10 actions (ingests, new pages, revisions)

Update the Overview after EVERY ingest or major edit. If you only update one page, it should be this one.

### Concepts (`/wiki/concepts/`) — ABSTRACT IDEAS
Pages for theoretical frameworks, methodologies, principles, themes — anything conceptual.
- `/wiki/concepts/scaling-laws.md`
- `/wiki/concepts/attention-mechanisms.md`
- `/wiki/concepts/self-supervised-learning.md`

Each concept page should: define the concept, explain why it matters in context, cite sources, and cross-reference related concepts and entities.

### Entities (`/wiki/entities/`) — CONCRETE THINGS
Pages for people, organizations, products, technologies, papers, datasets — anything you can point to.
- `/wiki/entities/transformer.md`
- `/wiki/entities/openai.md`
- `/wiki/entities/attention-is-all-you-need.md`

Each entity page should: describe what it is, note key facts, cite sources, and cross-reference related concepts and entities.

### Log (`/wiki/log.md`) — CHRONOLOGICAL RECORD
Always exists. Append-only. Records every ingest, major edit, and lint pass. Never delete entries.

Format — each entry starts with a parseable header:
```
## [YYYY-MM-DD] ingest | Source Title
- Created concept page: [Page Title](concepts/page.md)
- Updated entity page: [Page Title](entities/page.md)
- Updated overview with new findings
- Key takeaway: one sentence summary

## [YYYY-MM-DD] query | Question Asked
- Created new page: [Page Title](concepts/page.md)
- Finding: one sentence answer

## [YYYY-MM-DD] lint | Health Check
- Fixed contradiction between X and Y
- Added missing cross-reference in Z
```

### Additional Pages
You can create pages outside of concepts/ and entities/ when needed:
- `/wiki/comparisons/x-vs-y.md` — for deep comparisons
- `/wiki/timeline.md` — for chronological narratives

But concepts/ and entities/ are the primary categories. When in doubt, file there.

## Page Hierarchy

Wiki pages use a parent/child hierarchy via paths:
- `/wiki/concepts.md` — parent page (optional; summarizes all concepts)
- `/wiki/concepts/attention.md` — child page

Parent pages summarize; child pages go deep. The UI renders this as an expandable tree.

## Writing Standards

**Wiki pages must be substantially richer than a chat response.** They are persistent, curated artifacts.

### Structure
- Start with a summary paragraph (no H1 — the title is rendered by the UI)
- Use `##` for major sections, `###` for subsections
- One idea per section. Bullet points for facts, prose for synthesis.

### Visual Elements — MANDATORY

**Every wiki page MUST include at least one visual element.** A page with only prose is incomplete.

**Mermaid diagrams** — use for ANY structured relationship:
- Flowcharts for processes, pipelines, decision trees
- Sequence diagrams for interactions, timelines
- Quadrant charts for comparisons, trade-off analyses
- Entity relationship diagrams for people, companies, concepts

````
```mermaid
graph LR
    A[Input] --> B[Process] --> C[Output]
```
````

**Mermaid gotchas — MUST follow:**
- Always **quote** node labels that contain `/`, `:`, `(`, `)`, or punctuation that mermaid treats as syntax. Write `A["/wiki/concepts/foo.md"]`, NOT `A[/wiki/concepts/foo.md]`. The unquoted form collides with mermaid's parallelogram-shape syntax (`[/text/]`) and the whole diagram fails to render.
- When you want a node to act as a link to another wiki page, do it with a human-readable label plus a `click` directive, not by stuffing the path into the label:
  ```mermaid
  graph TD
      OV["Overview"]
      ARCH["Architecture"]
      OV --> ARCH
      click ARCH href "/wiki/concepts/architecture.md"
  ```
- Keep each diagram focused. If a flowchart has more than ~15 nodes it becomes too wide to read — split it into multiple smaller diagrams by topic.

**Tables** — use for ANY structured comparison:
- Feature matrices, pros/cons, timelines, metrics
- If you're listing 3+ items with attributes, it should be a table

**SVG assets** — for custom visuals Mermaid can't express:
- Create: `write(command="create", path="/wiki/", title="diagram.svg", content="<svg>...</svg>", tags=["diagram"])`
- Embed in wiki pages: `![Description](diagram.svg)`

### Citations — REQUIRED

Every factual claim MUST cite its source via markdown footnotes:
```
Transformers use self-attention[^1] that scales quadratically[^2].

[^1]: attention-paper.pdf, p.3
[^2]: scaling-laws.pdf, p.12-14
```

Rules:
- Use the FULL source filename — never truncate
- Add page numbers for PDFs: `paper.pdf, p.3`
- One citation per claim — don't batch unrelated claims
- Citations render as hoverable popover badges in the UI

### Cross-References — MANDATORY

**Every wiki page MUST link to at least 2 related pages.** Isolated pages are incomplete — they defeat the purpose of a wiki, which is a graph of knowledge the LLM can traverse.

Rules:
- Every **concept** page links to at least 2 related **entities** (the concrete things the concept touches).
- Every **entity** page links to at least 2 related **concepts** (the ideas that explain what the entity is or does).
- Link syntax: `[Page Title](/wiki/concepts/foo.md)` or `[Page Title](/wiki/entities/bar.md)` — always use the full `/wiki/...` path so the UI can resolve it.
- Put links **inline in prose** wherever you mention a related page by name, AND collect them at the bottom of every page in a `## See Also` section:
```
## See Also
- [Life Agent](/wiki/concepts/life-agent.md) — the orchestration layer this concept feeds into
- [Be Device](/wiki/entities/be-device.md) — the hardware that captures the signal
- [Alon Gamzu](/wiki/entities/alon-gamzu.md) — the team lead championing this direction
```
- If a page has nothing to link to, you probably missed the point of the source — re-read it and find the connections.

## Core Workflows

### Ingest a New Source
1. Read it: `read(path="source.pdf", pages="1-10")`
2. Discuss key takeaways with the user
3. Create or update **concept** pages under `/wiki/concepts/`
4. Create or update **entity** pages under `/wiki/entities/`
5. Update `/wiki/overview.md` — source count, key findings, recent updates
6. Append an entry to `/wiki/log.md`
7. A single source typically touches 5-15 wiki pages — that's expected

### Answer a Question
1. `search(mode="search", query="term")` to find relevant content
2. Read relevant wiki pages and sources
3. Synthesize with citations
4. If the answer is valuable, file it as a new wiki page — explorations should compound
5. Append a query entry to `/wiki/log.md`

### Maintain the Wiki (Lint)
Check for: contradictions, orphan pages, missing cross-references, stale claims, concepts mentioned but lacking their own page. Append a lint entry to `/wiki/log.md`.

## Available Knowledge Bases

"""


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="guide",
        description=(
            "Get started with LLM Wiki. Call this to understand how the knowledge vault works and see your available knowledge bases.\n\n"
            "Recommended workflow:\n"
            "1. Call `list_knowledge_bases` to discover available KBs and their slugs.\n"
            "2. Pass `kb_slug` to `search`, `read`, `write`, or `delete`.\n"
            "3. Optionally call `get_kb_guidelines(kb_slug)` before writing to understand editorial standards."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
    )
    async def guide(ctx: Context) -> str:
        user_id = get_user_id(ctx)
        kbs = await scoped_query(
            user_id,
            "SELECT name, slug, "
            "  (SELECT count(*) FROM documents d WHERE d.knowledge_base_id = kb.id AND d.path NOT LIKE '/wiki/%%' AND NOT d.archived) as source_count, "
            "  (SELECT count(*) FROM documents d WHERE d.knowledge_base_id = kb.id AND d.path LIKE '/wiki/%%' AND NOT d.archived) as wiki_count "
            "FROM knowledge_bases kb ORDER BY created_at DESC",
        )
        if not kbs:
            return GUIDE_TEXT + "No knowledge bases yet. Create one at " + settings.APP_URL + "/wikis"

        lines = []
        for kb in kbs:
            lines.append(f"- **{kb['name']}** (`{kb['slug']}`) — {kb['source_count']} sources, {kb['wiki_count']} wiki pages")
        return GUIDE_TEXT + "\n".join(lines)
