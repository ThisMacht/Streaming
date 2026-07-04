"""Consistent terminal logging."""

import logging

from rich.logging import RichHandler


def get_logger(name: str) -> logging.Logger:
    """Return a Rich logger without installing duplicate handlers."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = RichHandler(rich_tracebacks=True, show_path=False)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
