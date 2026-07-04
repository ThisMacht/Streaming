"""Discover Python source files to process."""

import json
from pathlib import Path
from typing import Annotated

import typer

from src.common.config import load_settings
from src.common.logging_utils import get_logger

EXCLUDE_PATTERNS = ["/tests/", "/test/", "/examples/", "/docs/", "setup.py", "__pycache__", ".venv"]
logger = get_logger(__name__)
app = typer.Typer(add_completion=False)


def should_exclude(path: Path) -> bool:
    normalized = f"/{path.as_posix().lstrip('/')}"
    return any(pattern in normalized for pattern in EXCLUDE_PATTERNS)


def discover_python_files(repo_path: Path, exclude: bool = True) -> list[Path]:
    """Return a deterministic list of Python files beneath repo_path."""
    if not repo_path.is_dir():
        raise FileNotFoundError(f"Repository path does not exist: {repo_path}")
    return sorted(
        path
        for path in repo_path.rglob("*.py")
        if path.is_file() and not (exclude and should_exclude(path))
    )


def save_discovered_files(files: list[Path], repo_path: Path, output_path: Path) -> None:
    """Write repo-relative file names to a discovery manifest."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "repo_path": repo_path.as_posix(),
        "total_files": len(files),
        "files": [path.resolve().relative_to(repo_path.resolve()).as_posix() for path in files],
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


@app.command()
def main(
    repo_path: Annotated[Path | None, typer.Option(help="Local repository root.")] = None,
    output_path: Annotated[Path, typer.Option()] = Path("data/processed/discovered_files.json"),
    include_excluded: Annotated[
        bool, typer.Option(help="Include tests, docs, and examples.")
    ] = False,
) -> None:
    settings = load_settings()
    root = repo_path or Path(settings.repo_local_path)
    files = discover_python_files(root, exclude=not include_excluded)
    save_discovered_files(files, root, output_path)
    logger.info("Discovered %d Python files", len(files))
    for path in files[:10]:
        logger.info("  %s", path.resolve().relative_to(root.resolve()))
    logger.info("Saved manifest to %s", output_path)


if __name__ == "__main__":
    app()
