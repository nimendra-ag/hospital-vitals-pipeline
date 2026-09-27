"""
Structured logging configuration using structlog.

Every pipeline component uses get_logger(component_name) to get a
logger that automatically attaches the component name, timestamp,
and log level to every emitted event — producing machine-parseable
JSON lines.
"""

import os
import sys
import logging
import structlog
from dotenv import load_dotenv

load_dotenv()

_configured = False


def configure_logging():
    """Configure structlog and stdlib logging. Idempotent."""
    global _configured
    if _configured:
        return
    _configured = True

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Configure stdlib logging (captures third-party library logs)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level, logging.INFO),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(component: str) -> structlog.BoundLogger:
    """
    Return a structlog logger bound to the given component name.

    Args:
        component: Pipeline component identifier,
                   e.g. "data_sources.vitals_producer"
    """
    configure_logging()
    return structlog.get_logger(component=component)
