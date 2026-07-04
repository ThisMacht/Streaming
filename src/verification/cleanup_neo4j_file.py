"""Delete CPG topology for exactly one repository file before replacement."""

from pathlib import Path
from typing import Annotated

import typer
from neo4j import GraphDatabase

from src.common.config import load_settings
from src.common.logging_utils import get_logger

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)


def cleanup_file(repo_name: str, file_path: str) -> int:
    """Delete and return the number of CPG nodes belonging to one source file."""
    settings = load_settings()
    query = """
    MATCH (n:CPGNode {repo_name: $repo_name, file_path: $file_path})
    WITH collect(n) AS nodes
    WITH nodes, size(nodes) AS deleted_nodes
    FOREACH (node IN nodes | DETACH DELETE node)
    RETURN deleted_nodes
    """
    with GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
    ) as driver:
        driver.verify_connectivity()
        record = driver.execute_query(
            query, repo_name=repo_name, file_path=file_path, database_="neo4j"
        ).records[0]
    return int(record["deleted_nodes"])


@app.command()
def main(file: Annotated[Path, typer.Option(help="Repository-relative Python file.")]) -> None:
    settings = load_settings()
    relative = file.as_posix()
    repo_path = Path(settings.repo_local_path).resolve()
    if file.is_absolute():
        relative = file.resolve().relative_to(repo_path).as_posix()
    deleted = cleanup_file(settings.repo_name, relative)
    logger.info(
        "Neo4j file cleanup: repo=%s file=%s nodes_deleted=%d",
        settings.repo_name,
        relative,
        deleted,
    )


if __name__ == "__main__":
    app()
