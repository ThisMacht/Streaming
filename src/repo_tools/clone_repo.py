"""Shallow-clone the configured source repository."""

import subprocess
from pathlib import Path
from typing import Annotated

import typer

from src.common.config import load_settings
from src.common.logging_utils import get_logger

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)


@app.command()
def main(
    repo_url: Annotated[str | None, typer.Option(help="Git repository URL.")] = None,
    target_path: Annotated[Path | None, typer.Option(help="Clone destination.")] = None,
) -> None:
    """Clone the repository with depth one, unless it already exists."""
    settings = load_settings()
    url = repo_url or settings.repo_url
    target = target_path or Path(settings.repo_local_path)
    if target.exists():
        logger.info("Repository path already exists; skipping clone: %s", target)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--depth", "1", url, str(target)], check=True)
    logger.info("Cloned %s into %s", url, target)


if __name__ == "__main__":
    app()
