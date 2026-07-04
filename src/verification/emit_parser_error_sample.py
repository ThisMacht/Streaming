"""Publish one controlled SyntaxError event without leaving an invalid source file behind."""

from pathlib import Path

from src.common.config import load_settings
from src.common.logging_utils import get_logger
from src.parser_service.kafka_producer import CpgKafkaProducer
from src.parser_service.main import process_file

logger = get_logger(__name__)
BROKEN_RELATIVE_PATH = Path("src/accelerate/_lab_parser_error.py")
BROKEN_SOURCE = "def broken_func(\n    return 1\n"


def main() -> None:
    settings = load_settings()
    repo_path = Path(settings.repo_local_path)
    target = repo_path / BROKEN_RELATIVE_PATH
    previous = target.read_bytes() if target.exists() else None
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(BROKEN_SOURCE, encoding="utf-8")
    producer = CpgKafkaProducer(settings.kafka_bootstrap_servers)
    try:
        success = process_file(settings, repo_path, target, producer, dry_run=False)
        producer.flush()
        if success:
            raise RuntimeError("Controlled invalid file unexpectedly parsed successfully")
        logger.info("Published controlled parser error for %s", BROKEN_RELATIVE_PATH)
    finally:
        if previous is None:
            target.unlink(missing_ok=True)
        else:
            target.write_bytes(previous)


if __name__ == "__main__":
    main()
