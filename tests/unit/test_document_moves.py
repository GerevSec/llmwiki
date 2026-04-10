from services.document_links import (
    build_document_location,
    rebase_relative_markdown_links,
    rewrite_markdown_links_to_target,
)


class TestDocumentLinkRewrites:
    def test_rebases_relative_links_inside_moved_document(self):
        content = "\n".join([
            "[Sibling](./sibling.md)",
            "[Root](../overview.md)",
            "![Sheet](./sheet.csv#tab=1)",
            "[External](https://example.com/docs)",
        ])

        rewritten = rebase_relative_markdown_links(
            content,
            build_document_location("/folder/", "moved.md"),
            build_document_location("/archive/", "moved.md"),
        )

        assert "[Sibling](../folder/sibling.md)" in rewritten
        assert "[Root](../overview.md)" in rewritten
        assert "![Sheet](../folder/sheet.csv#tab=1)" in rewritten
        assert "[External](https://example.com/docs)" in rewritten

    def test_rewrites_inbound_relative_links_to_moved_target(self):
        content = "\n".join([
            "[Moved](folder/moved.md)",
            "![Moved asset](folder/moved.md#section)",
            "[Other](folder/other.md)",
        ])

        rewritten = rewrite_markdown_links_to_target(
            content,
            build_document_location("/", "index.md"),
            build_document_location("/folder/", "moved.md"),
            build_document_location("/archive/", "moved.md"),
        )

        assert "[Moved](archive/moved.md)" in rewritten
        assert "![Moved asset](archive/moved.md#section)" in rewritten
        assert "[Other](folder/other.md)" in rewritten

    def test_rewrites_absolute_links_to_moved_target(self):
        content = "[Moved](/folder/moved.md?view=full#section)"

        rewritten = rewrite_markdown_links_to_target(
            content,
            build_document_location("/notes/", "reader.md"),
            build_document_location("/folder/", "moved.md"),
            build_document_location("/archive/", "moved.md"),
        )

        assert rewritten == "[Moved](/archive/moved.md?view=full#section)"

    def test_rebases_self_links_to_new_location(self):
        content = "[Self](moved.md#section)\n[Self dot](./moved.md)"

        rewritten = rebase_relative_markdown_links(
            content,
            build_document_location("/folder/", "moved.md"),
            build_document_location("/archive/", "moved.md"),
        )

        assert "[Self](moved.md#section)" in rewritten
        assert "[Self dot](./moved.md)" in rewritten

    def test_rewrites_targets_with_parentheses_in_filename(self):
        content = "[Moved](folder/my(file).md)\n![Moved](folder/my(file).md#page=2)"

        rewritten = rewrite_markdown_links_to_target(
            content,
            build_document_location("/", "index.md"),
            build_document_location("/folder/", "my(file).md"),
            build_document_location("/archive/", "my(file).md"),
        )

        assert "[Moved](archive/my(file).md)" in rewritten
        assert "![Moved](archive/my(file).md#page=2)" in rewritten

    def test_leaves_non_matching_or_external_links_unchanged(self):
        content = "\n".join([
            "[Mail](mailto:test@example.com)",
            "[Anchor](#local)",
            "[Other](../folder/other.md)",
        ])

        rewritten = rewrite_markdown_links_to_target(
            content,
            build_document_location("/notes/", "reader.md"),
            build_document_location("/folder/", "moved.md"),
            build_document_location("/archive/", "moved.md"),
        )

        assert rewritten == content
