from __future__ import annotations

import json
import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "step", "reference_date", "duration_s", "counts",
            "severity", "violations", "description",
        ):
            if hasattr(record, key):
                data[key] = getattr(record, key)
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    return logger


@contextmanager
def timed_step(
    logger: logging.Logger,
    step: str,
    reference_date: str,
) -> Generator[None, None, None]:
    t0 = time.perf_counter()
    logger.info("start", extra={"step": step, "reference_date": reference_date})
    try:
        yield
    except Exception:
        duration = round(time.perf_counter() - t0, 3)
        logger.exception(
            "failed",
            extra={"step": step, "reference_date": reference_date, "duration_s": duration},
        )
        raise
    else:
        duration = round(time.perf_counter() - t0, 3)
        logger.info(
            "done",
            extra={"step": step, "reference_date": reference_date, "duration_s": duration},
        )
