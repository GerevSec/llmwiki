"""Unit tests for the pre-deploy migration runner.

The runner is intentionally small. These tests pin the pure-function
behavior: discover SQL files in deterministic order and only treat
*.sql files as migrations.
"""

from __future__ import annotations

from pathlib import Path

from scripts.run_migrations import discover_migrations


def test_discover_migrations_returns_sql_files_in_lex_order(tmp_path: Path):
    (tmp_path / "002_b.sql").write_text("-- b")
    (tmp_path / "001_a.sql").write_text("-- a")
    (tmp_path / "010_c.sql").write_text("-- c")
    (tmp_path / "README.md").write_text("# ignore me")

    found = discover_migrations(tmp_path)

    assert [p.name for p in found] == ["001_a.sql", "002_b.sql", "010_c.sql"]


def test_discover_migrations_empty_dir_returns_empty_list(tmp_path: Path):
    assert discover_migrations(tmp_path) == []


def test_discover_migrations_missing_dir_returns_empty_list(tmp_path: Path):
    missing = tmp_path / "does-not-exist"
    assert discover_migrations(missing) == []


def test_discover_migrations_finds_real_repo_migrations():
    """Smoke: the script can locate the repo's actual migrations directory.
    Pins that we have at least the foundational migrations in source."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    found = discover_migrations(repo_root / "api" / "db_migrations")

    names = [p.name for p in found]
    assert "001_initial.sql" in names
    assert "010_kb_directives_unify.sql" in names
    # Must be sorted (lex order = numeric order due to zero-padding)
    assert names == sorted(names)
