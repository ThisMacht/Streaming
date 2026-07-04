"""Print basic Neo4j CPG statistics."""

from typing import Any

from neo4j import GraphDatabase

from src.common.config import load_settings
from src.common.logging_utils import get_logger

logger = get_logger(__name__)


def get_counts() -> dict[str, int]:
    """Return total CPG node and edge counts."""
    settings = load_settings()
    with GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
    ) as driver:
        driver.verify_connectivity()
        node_count = driver.execute_query(
            "MATCH (n:CPGNode) RETURN count(n) AS node_count", routing_="r"
        ).records[0]["node_count"]
        edge_count = driver.execute_query(
            "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS edge_count", routing_="r"
        ).records[0]["edge_count"]
    return {"node_count": node_count, "edge_count": edge_count}


def top_files() -> list[dict[str, Any]]:
    settings = load_settings()
    with GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
    ) as driver:
        result = driver.execute_query(
            """MATCH (f:SourceFile)-[:CONTAINS_NODE]->(n:CPGNode)
            RETURN f.path AS file_path, count(n) AS node_count
            ORDER BY node_count DESC LIMIT 10""",
            routing_="r",
        )
    return [record.data() for record in result.records]


def main() -> None:
    counts = get_counts()
    logger.info("Neo4j CPG nodes: %d", counts["node_count"])
    logger.info("Neo4j CPG edges: %d", counts["edge_count"])
    logger.info("Top files by node count:")
    for row in top_files():
        logger.info("  %s: %d", row["file_path"], row["node_count"])


if __name__ == "__main__":
    main()
