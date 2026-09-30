"""
Prometheus metrics for short-lived / non-HTTP pipeline components.

The FastAPI server can be scraped directly (see observability/api_metrics.py),
but the streaming producer, the daily lab generator, the Spark speed layer
and the Airflow-triggered batch layer are not HTTP servers. Each of them
periodically pushes its counters to a Prometheus Pushgateway instead, using
the metric objects and helpers defined here.

Every component also reports a heartbeat timestamp. Prometheus alert rules
compare `time() - hospital_component_heartbeat_timestamp_seconds` against a
threshold to detect a component that has silently stopped producing data —
this is what implements the "no data received in N minutes" health check
required by the project spec, at the infrastructure level rather than just
inside the application.
"""

import os
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

from observability.logging_config import get_logger

logger = get_logger("observability.prometheus_metrics")

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "localhost:9091")

registry = CollectorRegistry()

# -- Streaming ingestion (bedside vitals producer) --------------------------
vitals_records_sent_total = Counter(
    "hospital_vitals_records_sent_total",
    "Vitals readings successfully produced to Kafka",
    registry=registry,
)
vitals_send_errors_total = Counter(
    "hospital_vitals_send_errors_total",
    "Vitals readings that failed to deliver to Kafka",
    registry=registry,
)
vitals_abnormal_emitted_total = Counter(
    "hospital_vitals_abnormal_emitted_total",
    "Deliberately abnormal vitals readings emitted by the simulator",
    registry=registry,
)

# -- Batch ingestion (daily lab results generator) ---------------------------
lab_files_generated_total = Counter(
    "hospital_lab_files_generated_total",
    "Daily lab-result files written to disk",
    registry=registry,
)
lab_records_generated_total = Counter(
    "hospital_lab_records_generated_total",
    "Lab result records written across all daily files",
    registry=registry,
)

# -- Speed layer (Spark Structured Streaming) --------------------------------
speed_aggregates_written_total = Counter(
    "hospital_speed_aggregates_written_total",
    "Windowed vitals aggregate rows written to PostgreSQL",
    registry=registry,
)
speed_raw_records_archived_total = Counter(
    "hospital_speed_raw_records_archived_total",
    "Raw vitals readings appended to the Parquet master dataset",
    registry=registry,
)
speed_alerts_written_total = Counter(
    "hospital_speed_alerts_written_total",
    "Threshold-breach alerts written to PostgreSQL by the streaming job",
    registry=registry,
)

# -- Batch layer (Airflow-triggered daily reconciliation) -------------------
batch_lab_records_ingested_total = Counter(
    "hospital_batch_lab_records_ingested_total",
    "Lab records ingested into PostgreSQL by the batch layer",
    registry=registry,
)
batch_risk_reports_written_total = Counter(
    "hospital_batch_risk_reports_written_total",
    "Per-patient risk rows written by the batch layer",
    registry=registry,
)
batch_raw_readings_processed_total = Counter(
    "hospital_batch_raw_readings_processed_total",
    "Raw vitals readings read from the master dataset by the batch layer",
    registry=registry,
)
batch_last_simulated_day = Gauge(
    "hospital_batch_last_simulated_day",
    "Most recent simulated day the batch layer has processed",
    registry=registry,
)
batch_run_duration_seconds = Gauge(
    "hospital_batch_run_duration_seconds",
    "Wall-clock duration of the most recent daily batch run",
    registry=registry,
)

# -- Cross-component heartbeat -----------------------------------------------
component_heartbeat_timestamp = Gauge(
    "hospital_component_heartbeat_timestamp_seconds",
    "Unix timestamp of the last time this component reported activity",
    ["component"],
    registry=registry,
)


def heartbeat(component: str):
    """Record that `component` is alive right now."""
    component_heartbeat_timestamp.labels(component=component).set(time.time())


def push(job: str):
    """
    Push the current process-local registry snapshot to the Pushgateway
    under the given job name. Best-effort: a Pushgateway outage must never
    take down ingestion or processing.
    """
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
    except Exception:
        logger.warning("pushgateway_unreachable", job=job, url=PUSHGATEWAY_URL)
