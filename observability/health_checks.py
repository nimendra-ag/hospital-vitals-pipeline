"""
Pipeline health checks and alert rules.

Runs periodic checks against the database and metric store to detect:
  1. Data staleness  — no vitals received for a patient in N seconds
  2. Error rate      — ingestion or processing errors above threshold
  3. Consumer lag    — Kafka consumer falling behind
"""

import time
import threading

from observability.logging_config import get_logger
from observability.metrics import metrics

logger = get_logger("observability.health_checks")

# Thresholds (seconds / counts)
NO_DATA_THRESHOLD = 60      # alert if no vitals for this many seconds
ERROR_RATE_THRESHOLD = 0.05 # alert if error rate exceeds 5 %
CHECK_INTERVAL = 30         # run health checks every N seconds


def check_data_staleness() -> list[dict]:
    """
    Query the database for patients whose last vitals reading
    is older than NO_DATA_THRESHOLD seconds.
    """
    from storage.db import execute_query

    stale_patients = execute_query(
        """
        SELECT
            p.patient_id,
            p.name,
            MAX(v.window_end) AS last_seen
        FROM patients p
        LEFT JOIN vitals_aggregates v ON p.patient_id = v.patient_id
        GROUP BY p.patient_id, p.name
        HAVING MAX(v.window_end) IS NULL
           OR MAX(v.window_end) < (NOW() AT TIME ZONE 'UTC') - INTERVAL '%s seconds'
        """,
        (NO_DATA_THRESHOLD,),
    )

    if not stale_patients:
        return []

    # Staleness is an ongoing condition, not a discrete event — don't open
    # a new alert for a patient who already has one unacknowledged, or
    # every 30s check cycle floods the alerts table for as long as the
    # condition persists.
    already_open = execute_query(
        "SELECT DISTINCT patient_id FROM alerts "
        "WHERE alert_type = 'DATA_STALENESS' AND acknowledged = FALSE"
    )
    already_open_ids = {row["patient_id"] for row in already_open}

    alerts = []
    for row in stale_patients:
        if row["patient_id"] in already_open_ids:
            continue
        alert = {
            "patient_id": row["patient_id"],
            "alert_type": "DATA_STALENESS",
            "severity": "WARNING",
            "message": (
                f"No vitals data received for patient {row['patient_id']} "
                f"({row['name']}) in the last {NO_DATA_THRESHOLD} seconds."
            ),
        }
        alerts.append(alert)
        logger.warning(
            "data_staleness_detected",
            patient_id=row["patient_id"],
            last_seen=str(row["last_seen"]),
        )

    return alerts


def check_error_rate() -> list[dict]:
    """
    Compare ingestion error count to total count from the metric store.
    """
    total = metrics.get_counter("vitals_producer", "records_sent")
    errors = metrics.get_counter("vitals_producer", "send_errors")

    if total == 0:
        return []

    error_rate = errors / total
    alerts = []

    if error_rate > ERROR_RATE_THRESHOLD:
        from storage.db import execute_query

        already_open = execute_query(
            "SELECT 1 FROM alerts WHERE alert_type = 'HIGH_ERROR_RATE' "
            "AND acknowledged = FALSE LIMIT 1"
        )
        if already_open:
            return []

        alert = {
            "patient_id": "SYSTEM",
            "alert_type": "HIGH_ERROR_RATE",
            "severity": "CRITICAL",
            "message": (
                f"Vitals ingestion error rate is {error_rate:.1%} "
                f"({int(errors)}/{int(total)}), "
                f"exceeding threshold of {ERROR_RATE_THRESHOLD:.0%}."
            ),
        }
        alerts.append(alert)
        logger.error(
            "high_error_rate",
            error_rate=round(error_rate, 4),
            total=total,
            errors=errors,
        )

    return alerts


def run_all_checks() -> list[dict]:
    """Execute every health check and return the combined alert list."""
    alerts = []
    alerts.extend(check_data_staleness())
    alerts.extend(check_error_rate())

    if not alerts:
        logger.info("health_check_passed")
    else:
        logger.warning("health_check_alerts", count=len(alerts))

    # Persist any new alerts
    if alerts:
        _persist_alerts(alerts)

    return alerts


def _persist_alerts(alerts: list[dict]):
    """Write health-check alerts to the alerts table."""
    from storage.db import execute_batch

    rows = [
        (
            a["patient_id"],
            a["alert_type"],
            a["severity"],
            a["message"],
            None,  # vital_name
            None,  # vital_value
            None,  # threshold_value
        )
        for a in alerts
    ]
    execute_batch(
        "INSERT INTO alerts "
        "(patient_id, alert_type, severity, message, vital_name, vital_value, threshold_value) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        rows,
    )


class HealthCheckDaemon(threading.Thread):
    """
    Background thread that runs health checks at a fixed interval.

    Usage:
        daemon = HealthCheckDaemon()
        daemon.start()
        # ... later ...
        daemon.stop()
    """

    def __init__(self, interval: int = CHECK_INTERVAL):
        super().__init__(daemon=True, name="HealthCheckDaemon")
        self._interval = interval
        self._stop_event = threading.Event()

    def run(self):
        logger.info("health_check_daemon_started", interval=self._interval)
        while not self._stop_event.is_set():
            try:
                run_all_checks()
                metrics.flush_to_db()
            except Exception:
                logger.exception("health_check_error")
            self._stop_event.wait(self._interval)

    def stop(self):
        self._stop_event.set()
        logger.info("health_check_daemon_stopped")
