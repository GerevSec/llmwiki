"""Unit tests for TUS upload path normalization."""

import pytest

from infra.tus import _normalize_upload_path


class TestNormalizeUploadPath:

    @pytest.mark.parametrize("raw,expected", [
        (None, "/"),
        ("", "/"),
        ("   ", "/"),
        ("/", "/"),
        ("foo", "/foo/"),
        ("/foo", "/foo/"),
        ("foo/", "/foo/"),
        ("/foo/", "/foo/"),
        ("foo/bar", "/foo/bar/"),
        ("/foo/bar/", "/foo/bar/"),
        ("/foo//bar/", "/foo/bar/"),
        ("  /foo/  ", "/foo/"),
        ("///nested///deep///", "/nested/deep/"),
    ])
    def test_normalizes(self, raw, expected):
        assert _normalize_upload_path(raw) == expected
