"""
Structured JSON Logger for the Metaflow Onboarding App (§9.3).
One JSON object per line:
  ts, level, request_id, user, event, action_id, run_id, duration_ms, outcome, detail
"""

from datetime import datetime, timezone
import json
import logging
import os
import sys
from typing import Any, Optional

from server.branding_generated import env_var

# Imported from the leaf branding module rather than from settings.py: setup_logging()
# runs at import time in app.py, before settings are loaded, and branding_generated has
# no imports outside the stdlib.
ENV_LOG_LEVEL = env_var("LOG_LEVEL")


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    _STANDARD_ATTRS = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message"
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for attr, val in record.__dict__.items():
            if attr not in self._STANDARD_ATTRS and not attr.startswith("_"):
                if val is not None:
                    payload[attr] = val
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level_name: Optional[str] = None) -> logging.Logger:
    """Initialize structured logging on root logger."""
    if level_name is None:
        level_name = os.environ.get(ENV_LOG_LEVEL, "INFO").upper()

    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("flowx_app")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger
