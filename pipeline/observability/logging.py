"""Structured JSON logging for pipeline runs."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        if hasattr(record, "pipeline_ctx"):
            entry.update(record.pipeline_ctx)
        if record.exc_info and record.exc_info[1]:
            entry["error"] = str(record.exc_info[1])
        return json.dumps(entry, default=str)


def get_logger(name: str = "pipeline", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    return logger


def log_with_context(logger: logging.Logger, level: int, msg: str, **ctx: object) -> None:
    record = logger.makeRecord(logger.name, level, "", 0, msg, (), None)
    record.pipeline_ctx = ctx  # type: ignore[attr-defined]
    logger.handle(record)
