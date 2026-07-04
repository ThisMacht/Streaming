"""Verify that an idle resumed Spark query does not duplicate metadata.

Start (or restart) the Spark metadata job with its existing checkpoint in
Terminal 1. In Terminal 2, run this command without publishing new events.
It records checkpoint artifacts and verifies that MongoDB's document count
remains unchanged during the observation window.
"""

import time
from pathlib import Path
from typing import Annotated

import typer
from pymongo import MongoClient

from src.common.config import load_settings

app = typer.Typer(add_completion=False)


def metadata_count() -> int:
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        return client[settings.mongodb_database][
            settings.mongodb_collection_metadata
        ].count_documents({})
    finally:
        client.close()


def checkpoint_artifacts(checkpoint: Path) -> list[str]:
    if not checkpoint.is_dir():
        return []
    return sorted(
        path.relative_to(checkpoint).as_posix()
        for directory in ("offsets", "commits", "sources")
        for path in (checkpoint / directory).rglob("*")
        if path.is_file() and not path.name.startswith(".")
    )


@app.command()
def main(
    sleep_seconds: Annotated[
        float, typer.Option(min=0, help="Idle observation window in seconds.")
    ] = 10.0,
    output: Annotated[
        Path | None, typer.Option(help="Optional evidence log to write.")
    ] = None,
) -> None:
    settings = load_settings()
    checkpoint = Path(settings.spark_checkpoint_location)
    artifacts_before = checkpoint_artifacts(checkpoint)
    before = metadata_count()
    time.sleep(sleep_seconds)
    after = metadata_count()
    artifacts_after = checkpoint_artifacts(checkpoint)

    passed = before == after and checkpoint.is_dir() and bool(artifacts_after)
    lines = [
        f"checkpoint_location={checkpoint}",
        f"checkpoint_exists={checkpoint.is_dir()}",
        f"checkpoint_artifacts_before={len(artifacts_before)}",
        f"checkpoint_artifacts_after={len(artifacts_after)}",
        f"metadata_count_before={before}",
        f"metadata_count_after={after}",
        (
            "result=PASSED checkpoint resumed without duplicating unchanged metadata"
            if passed
            else "result=FAILED checkpoint missing or metadata count changed"
        ),
    ]
    rendered = "\n".join(lines) + "\n"
    typer.echo(rendered, nl=False)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    if not passed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

