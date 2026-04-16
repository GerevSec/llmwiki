"""Route prefix sanity tests.

These prevent regressions where new routers use a prefix that collides with
Next.js-reserved paths (e.g. `/api/*`) or otherwise diverges from the rest of
the API surface, which causes browser fetch() to fail with cryptic CORS errors
instead of clean HTTP responses.
"""

from __future__ import annotations

from routes.kb_guidelines import router as kb_guidelines_router
from routes.wiki_comments import router as wiki_comments_router


def _route_paths(router) -> set[str]:
    return {route.path for route in router.routes}


def test_kb_guidelines_router_uses_v1_knowledge_bases_prefix():
    assert kb_guidelines_router.prefix == "/v1/knowledge-bases"


def test_wiki_comments_router_uses_v1_knowledge_bases_prefix():
    assert wiki_comments_router.prefix == "/v1/knowledge-bases"


def test_kb_guidelines_route_paths_are_kb_scoped():
    paths = _route_paths(kb_guidelines_router)
    expected = {
        "/v1/knowledge-bases/{kb_id}/guidelines",
        "/v1/knowledge-bases/{kb_id}/guidelines/batch",
        "/v1/knowledge-bases/{kb_id}/guidelines/{guideline_id}",
    }
    assert expected.issubset(paths), f"missing routes: {expected - paths}"


def test_wiki_comments_route_paths_are_kb_scoped():
    paths = _route_paths(wiki_comments_router)
    expected = {
        "/v1/knowledge-bases/{kb_id}/pages/{page_key}/comments",
        "/v1/knowledge-bases/{kb_id}/comments/{comment_id}/archive",
        "/v1/knowledge-bases/{kb_id}/comments/{comment_id}/promote",
    }
    assert expected.issubset(paths), f"missing routes: {expected - paths}"


def test_kb_guidelines_router_exposes_batch_create():
    """Multi-add UX requires a single batch endpoint so N guidelines land
    atomically in one request."""
    paths_methods = {(route.path, frozenset(route.methods)) for route in kb_guidelines_router.routes}
    assert ("/v1/knowledge-bases/{kb_id}/guidelines/batch", frozenset({"POST"})) in paths_methods


def test_guideline_batch_create_preserves_multiline_bodies():
    """Each guideline can be free-form multi-line markdown including
    sub-bullets — internal whitespace must NOT be stripped, only outer."""
    from routes.kb_guidelines import GuidelineBatchCreate

    multiline = "- A\n- B\n  - B.1\n  - B.2\n- C"
    payload = GuidelineBatchCreate(bodies=["  " + multiline + "  ", "single line"])
    assert payload.bodies[0].strip() == multiline, "outer whitespace OK to strip; internal newlines must survive"
    # Behavior contract: handler trims outer whitespace, drops empty entries,
    # but preserves multi-line content. Smoke-asserts the model accepts both.
    assert "\n  - B.1\n" in payload.bodies[0]


def test_no_route_under_api_prefix():
    """Next.js reserves `/api/*` for its own internal routes. Backend routers
    must not collide — same-origin browser fetches to `/api/...` get routed to
    Next, which 404s without CORS headers, producing a confusing
    'Could not reach the API' error instead of a clean response."""
    for router in (kb_guidelines_router, wiki_comments_router):
        for route in router.routes:
            assert not route.path.startswith("/api/"), (
                f"route {route.path} on router {router.tags} uses reserved /api/ prefix"
            )
