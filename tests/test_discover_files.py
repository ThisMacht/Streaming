"""Path-aware repository discovery tests."""

from pathlib import Path

from src.repo_tools.discover_files import discover_python_files, should_exclude


def test_excludes_tests_directory_component() -> None:
    assert should_exclude(Path("package/tests/helpers.py"))


def test_excludes_test_utils_directory_component() -> None:
    assert should_exclude(Path("package/test_utils/helpers.py"))


def test_excludes_test_prefixed_python_filename() -> None:
    assert should_exclude(Path("package/test_feature.py"))


def test_includes_normal_source_file_containing_test_text() -> None:
    assert not should_exclude(Path("package/latest_release/contest.py"))


def test_discovery_order_is_deterministic(tmp_path: Path) -> None:
    for relative in ("zeta.py", "alpha.py", "nested/middle.py"):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("value = 1\n", encoding="utf-8")

    first = discover_python_files(tmp_path)
    second = discover_python_files(tmp_path)

    assert first == second == sorted(first)
