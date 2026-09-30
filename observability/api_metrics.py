"""
Prometheus metrics for the serving layer (FastAPI).

Unlike observability/prometheus_metrics.py (which is pushed to the
Pushgateway by short-lived components), the API process is long-running
and is scraped by Prometheus directly at GET /metrics. Business gauges
here are recomputed from PostgreSQL on every scrape rather than cached,
since scrape interval (10s) already bounds staleness.
"""

import time
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Gauge, generate_latest

from observability.logging_config import get_logger

logger = get_logger("observability.api_metrics")

registry = CollectorRegistry()

ALL_SEVERITIES = ["CRITICAL", "WARNING"]
ALL_RISK_LEVELS = ["LOW", "MODERATE", "HIGH", "CRITICAL"]

active_patients = Gauge(
    "hospital_active_patients", "Total registered patients", registry=registry
)
active_alerts = Gauge(
    "hospital_active_alerts",
    "Unacknowledged alerts, by severity",
    ["severity"],
    registry=registry,
)
patients_by_risk_level = Gauge(
    "hospital_patients_by_risk_level",
    "Patients in the latest daily risk report, by risk level",
    ["risk_level"],
    registry=registry,
)
avg_combined_risk_score = Gauge(
    "hospital_avg_combined_risk_score",
    "Average combined risk score across the latest daily report",
    registry=registry,
)
oldest_vitals_reading_age_seconds = Gauge(
    "hospital_oldest_vitals_reading_age_seconds",
    "Seconds since the least-recently-updated patient's last vitals window "
    "(-1 if no vitals have arrived yet)",
    registry=registry,
)
component_heartbeat_timestamp = Gauge(
    "hospital_component_heartbeat_timestamp_seconds",
    "Unix timestamp of the last time this component reported activity",
    ["component"],
    registry=registry,
)


def collect_live_metrics() -> bytes:
    """Query current pipeline state and render it in Prometheus text format."""
    from storage.db import execute_query

    try:
        patients = execute_query("SELECT COUNT(*) AS cnt FROM patients")
        active_patients.set(patients[0]["cnt"] if patients else 0)

        alert_rows = execute_query(
            "SELECT severity, COUNT(*) AS cnt FROM alerts "
            "WHERE acknowledged = FALSE GROUP BY severity"
        )
        counts = {row["severity"]: row["cnt"] for row in alert_rows}
        for sev in ALL_SEVERITIES:
            active_alerts.labels(severity=sev).set(counts.get(sev, 0))

        risk_rows = execute_query(
            """
            SELECT risk_level, COUNT(*) AS cnt, AVG(combined_risk_score) AS avg_score
            FROM patient_risk
            WHERE simulated_day = (SELECT MAX(simulated_day) FROM patient_risk)
            GROUP BY risk_level
            """
        )
        level_counts = {row["risk_level"]: row["cnt"] for row in risk_rows}
        for level in ALL_RISK_LEVELS:
            patients_by_risk_level.labels(risk_level=level).set(level_counts.get(level, 0))

        if risk_rows:
            total = sum(r["cnt"] for r in risk_rows)
            weighted = sum((r["avg_score"] or 0) * r["cnt"] for r in risk_rows)
            avg_combined_risk_score.set(round(weighted / total, 2) if total else 0)
        else:
            avg_combined_risk_score.set(0)

        staleness_rows = execute_query(
            "SELECT MIN(last_seen) AS oldest FROM ("
            "  SELECT patient_id, MAX(window_end) AS last_seen "
            "  FROM vitals_aggregates GROUP BY patient_id"
            ") t"
        )
        oldest = staleness_rows[0]["oldest"] if staleness_rows else None
        if oldest:
            # window_end is stored as naive UTC (Spark session timezone = UTC).
            age = (datetime.now(timezone.utc) - oldest.replace(tzinfo=timezone.utc)).total_seconds()
            oldest_vitals_reading_age_seconds.set(max(age, 0))
        else:
            oldest_vitals_reading_age_seconds.set(-1)

        component_heartbeat_timestamp.labels(component="api").set(time.time())
    except Exception:
        logger.exception("metrics_collection_failed")

    return generate_latest(registry)
