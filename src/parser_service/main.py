"""CLI for parsing and publishing one file at a time."""

import json
from pathlib import Path
from typing import Annotated

import typer

from src.common.config import Settings, load_settings
from src.common.logging_utils import get_logger
from src.parser_service.event_builder import build_error_event, build_events_for_file
from src.parser_service.kafka_producer import CpgKafkaProducer
from src.repo_tools.discover_files import discover_python_files, save_discovered_files

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)
MANIFEST_PATH = Path("data/processed/discovered_files.json")


def _display_path(repo_path: Path, file_path: Path) -> str:
    root = repo_path.resolve()
    candidate = file_path.resolve()
    if not file_path.is_absolute() and not candidate.is_relative_to(root):
        candidate = (root / file_path).resolve()
    try:
        return candidate.relative_to(root).as_posix()
    except ValueError:
        return file_path.as_posix()


def _files_for_all(repo_path: Path) -> list[Path]:
    if not MANIFEST_PATH.exists():
        files = discover_python_files(repo_path)
        save_discovered_files(files, repo_path, MANIFEST_PATH)
        return files
    payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    entries = payload.get("files")
    if not isinstance(entries, list) or not all(isinstance(item, str) for item in entries):
        raise ValueError(f"Invalid discovery manifest: {MANIFEST_PATH}")
    return [repo_path / item for item in entries]


def process_file(
    settings: Settings,
    repo_path: Path,
    file_path: Path,
    producer: CpgKafkaProducer | None,
    dry_run: bool,
) -> bool:
    """Build and optionally publish events for one file; return success status."""
    display_path = _display_path(repo_path, file_path)
    try:
        nodes, edges, metadata = build_events_for_file(settings.repo_name, repo_path, file_path)
        if not dry_run:
            assert producer is not None
            for event in nodes:
                producer.send_node(settings.kafka_topic_nodes, event)
            for event in edges:
                producer.send_edge(settings.kafka_topic_edges, event)
            producer.send_metadata(settings.kafka_topic_metadata, metadata)
        logger.info(
            "Parsed %s: nodes=%d edges=%d metadata=1%s",
            display_path,
            len(nodes),
            len(edges),
            " (dry-run)" if dry_run else "",
        )
        return True
    except Exception as exc:  # one malformed file must not terminate a repository run
        logger.exception("Failed to parse %s", display_path)
        if not dry_run and producer is not None:
            error = build_error_event(settings.repo_name, display_path, exc)
            producer.send_error(settings.kafka_topic_errors, error)
        return False


@app.command()
def main(
    repo_name: Annotated[str | None, typer.Option()] = None,
    repo_path: Annotated[Path | None, typer.Option()] = None,
    mode: Annotated[str, typer.Option(help="Processing mode: all or one.")] = "all",
    file: Annotated[
        Path | None, typer.Option(help="Repository-relative Python file for mode=one.")
    ] = None,
    dry_run: Annotated[
        bool, typer.Option(help="Build events without publishing to Kafka.")
    ] = False,
) -> None:
    """Parse all discovered files or one selected source file."""
    loaded = load_settings()
    if mode not in {"all", "one"}:
        raise typer.BadParameter("--mode must be 'all' or 'one'")
    settings = Settings(**{**loaded.__dict__, **({"repo_name": repo_name} if repo_name else {})})
    root = repo_path or Path(settings.repo_local_path)
    if mode == "one" and file is None:
        raise typer.BadParameter("--file is required when --mode one")
    files = _files_for_all(root) if mode == "all" else [file]  # type: ignore[list-item]
    producer = None if dry_run else CpgKafkaProducer(settings.kafka_bootstrap_servers)
    succeeded = 0
    try:
        for source_file in files:
            succeeded += process_file(settings, root, source_file, producer, dry_run)
    finally:
        if producer is not None:
            producer.flush()
    logger.info("Finished: successful=%d failed=%d", succeeded, len(files) - succeeded)


if __name__ == "__main__":
    app()
