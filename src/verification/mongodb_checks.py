"""Print MongoDB metadata statistics and duplicate checks."""

from typing import Any

from pymongo import DESCENDING, MongoClient

from src.common.config import load_settings
from src.common.logging_utils import get_logger

logger = get_logger(__name__)

METADATA_ID_DUPLICATES = [
    {
        "$group": {
            "_id": "$metadata_id",
            "count": {"$sum": 1},
            "files": {"$addToSet": "$file_path"},
        }
    },
    {"$match": {"count": {"$gt": 1}}},
    {"$limit": 10},
]

REPO_FILE_DUPLICATES = [
    {
        "$group": {
            "_id": {"repo_name": "$repo_name", "file_path": "$file_path"},
            "count": {"$sum": 1},
        }
    },
    {"$match": {"count": {"$gt": 1}}},
    {"$limit": 10},
]


def get_metadata(file_path: str, repo_name: str | None = None) -> dict[str, Any] | None:
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        collection = client[settings.mongodb_database][settings.mongodb_collection_metadata]
        return collection.find_one(
            {"repo_name": repo_name or settings.repo_name, "file_path": file_path}, {"_id": 0}
        )
    finally:
        client.close()


def get_metadata_count() -> int:
    """Return the number of metadata documents."""
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        return client[settings.mongodb_database][
            settings.mongodb_collection_metadata
        ].count_documents({})
    finally:
        client.close()


def get_duplicate_groups() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return duplicate groups by metadata ID and repository/file identity."""
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        collection = client[settings.mongodb_database][settings.mongodb_collection_metadata]
        return (
            list(collection.aggregate(METADATA_ID_DUPLICATES)),
            list(collection.aggregate(REPO_FILE_DUPLICATES)),
        )
    finally:
        client.close()


def main() -> None:
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri)
    try:
        collection = client[settings.mongodb_database][settings.mongodb_collection_metadata]
        logger.info("MongoDB metadata documents: %d", collection.count_documents({}))
        logger.info("10 most recently updated files:")
        for doc in (
            collection.find({}, {"_id": 0, "file_path": 1, "event_time": 1})
            .sort("event_time", DESCENDING)
            .limit(10)
        ):
            logger.info("  %s (%s)", doc.get("file_path"), doc.get("event_time"))
        metadata_duplicates = list(collection.aggregate(METADATA_ID_DUPLICATES))
        repo_file_duplicates = list(collection.aggregate(REPO_FILE_DUPLICATES))
        if not metadata_duplicates and not repo_file_duplicates:
            logger.info("No duplicate metadata documents found.")
        else:
            logger.warning("Duplicate metadata_id groups: %d", len(metadata_duplicates))
            for duplicate in metadata_duplicates:
                logger.warning("  %s", duplicate)
            logger.warning("Duplicate repo_name + file_path groups: %d", len(repo_file_duplicates))
            for duplicate in repo_file_duplicates:
                logger.warning("  %s", duplicate)
    finally:
        client.close()


if __name__ == "__main__":
    main()
