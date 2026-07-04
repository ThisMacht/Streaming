"""Verify MongoDB metadata uniqueness and print a target document."""

from pathlib import Path
from typing import Annotated, Any

import typer
from pymongo import MongoClient

from src.common.config import load_settings
from src.common.logging_utils import get_logger
from src.verification.mongodb_checks import METADATA_ID_DUPLICATES, REPO_FILE_DUPLICATES

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)

DISPLAY_FIELDS = {
    "_id": 0,
    "metadata_id": 1,
    "file_path": 1,
    "file_hash": 1,
    "event_time": 1,
    "ingested_at": 1,
    "spark_batch_id": 1,
    "node_count": 1,
    "edge_count": 1,
}


@app.command()
def main(
    file: Annotated[Path | None, typer.Option(help="Repository-relative target file.")] = None,
) -> None:
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        collection = client[settings.mongodb_database][settings.mongodb_collection_metadata]
        duplicates_by_id: list[dict[str, Any]] = list(collection.aggregate(METADATA_ID_DUPLICATES))
        duplicates_by_file: list[dict[str, Any]] = list(collection.aggregate(REPO_FILE_DUPLICATES))
        logger.info("MongoDB metadata documents: %d", collection.count_documents({}))
        logger.info("Duplicate metadata_id groups: %d", len(duplicates_by_id))
        logger.info("Duplicate repo/file groups: %d", len(duplicates_by_file))
        if file is not None:
            relative = file.as_posix()
            if file.is_absolute():
                relative = (
                    file.resolve().relative_to(Path(settings.repo_local_path).resolve()).as_posix()
                )
            document = collection.find_one(
                {"repo_name": settings.repo_name, "file_path": relative}, DISPLAY_FIELDS
            )
            logger.info("Target metadata: %s", document)
        if duplicates_by_id or duplicates_by_file:
            raise typer.Exit(code=1)
    finally:
        client.close()


if __name__ == "__main__":
    app()
