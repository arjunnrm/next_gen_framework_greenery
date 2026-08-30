"""
Structured JSON Logger for Metaflow Onboarding App (§9.3).
One JSON object per line:
  ts, level, request_id, user, event, action_id, run_id, duration_ms, outcome, detail
"""

from datetime import datetime, timezone
import json
import logging
import os
import sys
from typing import Any, Optional


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for attr in ("request_id", "user", "event", "action_id", "run_id", "duration_ms", "outcome", "detail"):
            val = getattr(record, attr, None)
            if val is not None:
                payload[attr] = val
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def setup_logging(level_name: Optional[str] = None) -> logging.Logger:
    """Initialize structured logging on root logger."""
    if level_name is None:
        level_name = os.environ.get("METAFLOW_LOG_LEVEL", "INFO").upper()

    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("metaflow_app")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger
