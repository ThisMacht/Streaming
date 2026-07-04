"""Verify Neo4j identity uniqueness and print global/per-file graph evidence."""

from pathlib import Path
from typing import Annotated

import typer
from neo4j import GraphDatabase

from src.common.config import load_settings
from src.common.logging_utils import get_logger

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)


@app.command()
def main(
    file: Annotated[Path | None, typer.Option(help="Repository-relative target file.")] = None,
) -> None:
    settings = load_settings()
    relative = file.as_posix() if file is not None else None
    if file is not None and file.is_absolute():
        relative = file.resolve().relative_to(Path(settings.repo_local_path).resolve()).as_posix()
    with GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
    ) as driver:
        driver.verify_connectivity()
        nodes = driver.execute_query(
            "MATCH (n:CPGNode) RETURN count(n) AS count",
            routing_="r",
        ).records[0]["count"]
        edges = driver.execute_query(
            "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS count",
            routing_="r",
        ).records[0]["count"]
        duplicate_nodes = driver.execute_query(
            """MATCH (n:CPGNode) WITH n.id AS id, count(*) AS c
            WHERE c > 1 RETURN id, c LIMIT 20""",
            routing_="r",
        ).records
        duplicate_edges = driver.execute_query(
            """MATCH ()-[r:CPG_EDGE]->() WITH r.id AS id, count(*) AS c
            WHERE id IS NOT NULL AND c > 1 RETURN id, c LIMIT 20""",
            routing_="r",
        ).records
        placeholders = driver.execute_query(
            "MATCH (n:CPGNode {placeholder: true}) RETURN count(n) AS count", routing_="r"
        ).records[0]["count"]
        logger.info("Neo4j totals: nodes=%d edges=%d", nodes, edges)
        logger.info("Duplicate node IDs: %d", len(duplicate_nodes))
        logger.info("Duplicate edge IDs: %d", len(duplicate_edges))
        logger.info("Unresolved placeholder nodes: %d", placeholders)
        if relative:
            file_result = (
                driver.execute_query(
                    """MATCH (n:CPGNode {repo_name: $repo_name, file_path: $file_path})
                OPTIONAL MATCH (n)-[r:CPG_EDGE]-()
                RETURN count(DISTINCT n) AS nodes, count(DISTINCT r) AS edges""",
                    repo_name=settings.repo_name,
                    file_path=relative,
                    routing_="r",
                )
                .records[0]
                .data()
            )
            samples = driver.execute_query(
                """MATCH (a:CPGNode {repo_name: $repo_name, file_path: $file_path})
                -[r:CPG_EDGE]->(b:CPGNode)
                RETURN a.id AS source_id, a.type AS source_type, r.id AS edge_id,
                       r.type AS edge_type, b.id AS target_id, b.type AS target_type LIMIT 5""",
                repo_name=settings.repo_name,
                file_path=relative,
                routing_="r",
            ).records
            logger.info(
                "Target file %s: nodes=%d edges=%d",
                relative,
                file_result["nodes"],
                file_result["edges"],
            )
            for record in samples:
                logger.info("Sample edge: %s", record.data())
        if duplicate_nodes or duplicate_edges or placeholders:
            raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
