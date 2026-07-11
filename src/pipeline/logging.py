"""Structured JSON logging.

Every log line is a single JSON object so it can be shipped to a log
aggregator (CloudWatch, Loki, Datadog) and queried by `run_id`,
`reference_date`, or `stage` without regex gymnastics. This is the primary
diagnostic surface for the Monday-morning troubleshooting runbook.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        # Structured context passed via `extra={"json": {...}}`. Canonical fields
        # (ts/level/logger/event) are protected so context can't overwrite them.
        ctx = getattr(record, "json", None)
        if isinstance(ctx, dict):
            reserved = {"ts", "level", "logger", "event"}
            payload.update({k: v for k, v in ctx.items() if k not in reserved})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "pipeline", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger
