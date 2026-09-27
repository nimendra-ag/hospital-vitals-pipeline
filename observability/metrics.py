"""
Pipeline metrics collection.

Tracks counters and gauges for each pipeline stage.  Metrics are
stored in-memory and periodically flushed to the pipeline_metrics
table so the API can expose them and the health-check logic can
query them.
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field

from observability.logging_config import get_logger

logger = get_logger("observability.metrics")


@dataclass
class MetricPoint:
    """A single metric observation."""
    value: float
    timestamp: float = field(default_factory=time.time)


class PipelineMetrics:
    """
    Thread-safe in-process metric store.

    Supports two metric types:
      - counter: monotonically increasing (e.g. records_ingested)
      - gauge:   point-in-time value   (e.g. consumer_lag)
    """

    def __init__(self):
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, MetricPoint] = {}
        self._lock = threading.Lock()

    # -- Counter operations --------------------------------------------------

    def increment(self, component: str, metric_name: str, value: float = 1.0):
        key = f"{component}.{metric_name}"
        with self._lock:
            self._counters[key] += value

    def get_counter(self, component: str, metric_name: str) -> float:
        key = f"{component}.{metric_name}"
        with self._lock:
            return self._counters.get(key, 0.0)

    # -- Gauge operations ----------------------------------------------------

    def set_gauge(self, component: str, metric_name: str, value: float):
        key = f"{component}.{metric_name}"
        with self._lock:
            self._gauges[key] = MetricPoint(value=value)

    def get_gauge(self, component: str, metric_name: str) -> float | None:
        key = f"{component}.{metric_name}"
        with self._lock:
            point = self._gauges.get(key)
            return point.value if point else None

    # -- Snapshot for API / health checks ------------------------------------

    def snapshot(self) -> dict:
        """Return a dict of all current metric values."""
        with self._lock:
            result = {}
            for key, value in self._counters.items():
                result[key] = {"type": "counter", "value": value}
            for key, point in self._gauges.items():
                result[key] = {
                    "type": "gauge",
                    "value": point.value,
                    "timestamp": point.timestamp,
                }
            return result

    def flush_to_db(self):
        """
        Persist current metrics to the pipeline_metrics table.

        Imported here to avoid circular imports at module load time.
        """
        from storage.db import execute_batch

        snapshot = self.snapshot()
        if not snapshot:
            return

        rows = []
        for key, info in snapshot.items():
            parts = key.split(".", 1)
            component = parts[0] if len(parts) > 1 else "unknown"
            metric_name = parts[1] if len(parts) > 1 else key
            rows.append((component, metric_name, info["value"]))

        execute_batch(
            "INSERT INTO pipeline_metrics (component, metric_name, metric_value) "
            "VALUES (%s, %s, %s)",
            rows,
        )
        logger.info("metrics_flushed", count=len(rows))


# Module-level singleton — import and use directly.
metrics = PipelineMetrics()
