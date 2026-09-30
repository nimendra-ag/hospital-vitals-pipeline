"""
Batch layer — daily patient risk reconciliation (PySpark batch job).

Runs once per simulated day, triggered by Airflow. For simulated day N it:

  1. Ingests lab_results_day_N.json (one report per patient, each with the
     full 5-test panel) into lab_results, one row per test (idempotent
     upsert, so an Airflow retry or manual re-run never duplicates rows).
  2. Recomputes that day's vitals picture FROM THE RAW MASTER DATASET
     (Parquet written by the speed layer's raw_archive query) with Spark:
       - cleaning: de-duplicate on event_id (the archive is at-least-once)
       - per patient: readings, % breaching clinical alert thresholds, least-squares
         heart-rate and SpO2 trend (slope per minute) over the day.
     It does NOT reuse the speed layer's aggregates: the batch view is an
     independent, replayable recomputation — the core Lambda property.
  3. Joins vitals features with the day's lab results per patient and
     scores risk with processing/clinical_rules.py.
  4. Upserts one patient_risk row per (patient_id, simulated_day).

The day's vitals period: the lab file is an end-of-day extract, so day N
covers the SIMULATED_DAY_SECONDS of vitals ending at the file's
collected_at timestamp.

Re-running any day (e.g. after changing the risk model) is just:
    python -m processing.batch_layer.daily_risk_processor --day N
"""

import os
import sys
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
import structlog

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from storage.db import execute_query, execute_batch  # noqa: E402
from observability.logging_config import get_logger  # noqa: E402
from observability.prometheus_metrics import (  # noqa: E402
    batch_lab_records_ingested_total,
    batch_risk_reports_written_total,
    batch_raw_readings_processed_total,
    batch_last_simulated_day,
    batch_run_duration_seconds,
    heartbeat,
    push as push_metrics,
)
from processing.clinical_rules import (  # noqa: E402
    ALERT_THRESHOLDS as T,
    vitals_risk_score,
    lab_risk_score,
    combine_risk,
)

logger = get_logger("processing.batch_layer")

LAB_DATA_DIR = os.getenv("LAB_DATA_DIR", "data/lab_results")
MASTER_DATASET_DIR = os.path.abspath(os.getenv("MASTER_DATASET_DIR", "data/master/vitals"))
SIMULATED_DAY_SECONDS = int(os.getenv("SIMULATED_DAY_SECONDS", "300"))


def lab_file_path(simulated_day: int) -> Path:
    return Path(LAB_DATA_DIR) / f"lab_results_day_{str(simulated_day).zfill(3)}.json"


# ── Step 1: ingest the daily lab file ─────────────────────────────

def ingest_lab_file(simulated_day: int) -> tuple[int, datetime]:
    """
    Upsert the day's lab file into lab_results.

    Returns (records ingested, the file's collected_at as naive UTC).
    """
    filepath = lab_file_path(simulated_day)
    if not filepath.exists():
        logger.error("lab_file_not_found", path=str(filepath), day=simulated_day)
        raise FileNotFoundError(f"Lab file not found: {filepath}")

    with open(filepath, "r") as f:
        records = json.load(f)

    # Each file holds one report per patient; each report holds the full
    # panel. Flatten to one row per test (the queryable shape), validating as
    # we go: a report without patient/collection time, or a test without a
    # type/value, is rejected rather than failing the whole day.
    rows, rejected, collected_times = [], 0, []
    for report in records:
        if not report.get("patient_id") or not report.get("collected_at"):
            rejected += 1
            continue
        collected_times.append(report["collected_at"])
        for t in report.get("tests", []):
            if t.get("test_type") is None or t.get("result_value") is None:
                rejected += 1
                continue
            rows.append((
                report["patient_id"], t["test_type"], t["result_value"],
                t.get("reference_min"), t.get("reference_max"),
                bool(t.get("is_abnormal")), report["collected_at"], simulated_day,
            ))
    if rejected:
        logger.warning("lab_records_rejected", day=simulated_day, rejected=rejected)

    execute_batch(
        """
        INSERT INTO lab_results
            (patient_id, test_type, result_value, reference_min,
             reference_max, is_abnormal, collected_at, simulated_day)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patient_id, test_type, simulated_day) DO UPDATE SET
            result_value = EXCLUDED.result_value,
            reference_min = EXCLUDED.reference_min,
            reference_max = EXCLUDED.reference_max,
            is_abnormal = EXCLUDED.is_abnormal,
            collected_at = EXCLUDED.collected_at,
            ingested_at = (NOW() AT TIME ZONE 'UTC')
        """,
        rows,
    )

    collected_at = max(
        datetime.fromisoformat(ts.replace("Z", "+00:00")) for ts in collected_times
    ).astimezone(timezone.utc).replace(tzinfo=None)

    logger.info("lab_file_ingested", day=simulated_day, records=len(rows), collected_at=str(collected_at))
    batch_lab_records_ingested_total.inc(len(rows))
    return len(rows), collected_at


# ── Step 2: vitals features from the raw master dataset (Spark) ──

def create_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("hospital-vitals-batch-layer")
        .master(os.getenv("SPARK_BATCH_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def compute_vitals_features(spark, period_start: datetime, period_end: datetime) -> dict[str, dict]:
    """
    Per-patient vitals features for [period_start, period_end) from the
    raw Parquet master dataset.

    Returns {patient_id: {"total", "abnormal", "hr_trend", "spo2_trend",
                          "avg_heart_rate", "min_spo2", "max_temperature"}}
    """
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    try:
        raw = spark.read.parquet(MASTER_DATASET_DIR)
    except AnalysisException:
        logger.warning("master_dataset_empty", path=MASTER_DATASET_DIR)
        return {}

    def breach(name):
        return (F.col(name) < T[f"{name}_low"]) | (F.col(name) > T[f"{name}_high"])

    minutes = (F.unix_timestamp("event_time") - F.lit(int(period_start.replace(tzinfo=timezone.utc).timestamp()))) / 60.0

    day = (
        raw
        # partition pruning first, then the exact time range
        .filter(F.col("event_date").between(period_start.date(), period_end.date()))
        .filter((F.col("event_time") >= F.lit(period_start)) & (F.col("event_time") < F.lit(period_end)))
        .dropDuplicates(["event_id"])
        .withColumn("abnormal", (
            breach("heart_rate") | (F.col("spo2") < T["spo2_low"])
            | breach("systolic_bp") | breach("temperature")
        ).cast("int"))
        .withColumn("t_min", minutes)
    )

    features = (
        day.groupBy("patient_id")
        .agg(
            F.count("*").alias("total"),
            F.sum("abnormal").alias("abnormal"),
            # least-squares slope = cov(t, y) / var(t)
            (F.covar_samp("t_min", "heart_rate") / F.var_samp("t_min")).alias("hr_trend"),
            (F.covar_samp("t_min", "spo2") / F.var_samp("t_min")).alias("spo2_trend"),
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.min("spo2").alias("min_spo2"),
            F.max("temperature").alias("max_temperature"),
        )
        .collect()
    )

    return {row["patient_id"]: row.asDict() for row in features}


# ── Step 3: lab results for the day ───────────────────────────────

def load_lab_results(simulated_day: int) -> dict[str, list[dict]]:
    rows = execute_query(
        """
        SELECT patient_id, test_type, result_value, reference_min,
               reference_max, is_abnormal
        FROM lab_results WHERE simulated_day = %s
        """,
        (simulated_day,),
    )
    by_patient: dict[str, list[dict]] = {}
    for r in rows:
        by_patient.setdefault(r["patient_id"], []).append(r)
    return by_patient


# ── Step 4: join, score, persist ──────────────────────────────────

UPSERT_RISK_SQL = """
    INSERT INTO patient_risk
        (patient_id, report_date, simulated_day, vitals_risk_score,
         lab_risk_score, combined_risk_score, risk_level,
         abnormal_vitals_count, abnormal_labs_count, risk_factors,
         readings_analyzed, heart_rate_trend, spo2_trend,
         period_start, period_end)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (patient_id, simulated_day) DO UPDATE SET
        report_date = EXCLUDED.report_date,
        vitals_risk_score = EXCLUDED.vitals_risk_score,
        lab_risk_score = EXCLUDED.lab_risk_score,
        combined_risk_score = EXCLUDED.combined_risk_score,
        risk_level = EXCLUDED.risk_level,
        abnormal_vitals_count = EXCLUDED.abnormal_vitals_count,
        abnormal_labs_count = EXCLUDED.abnormal_labs_count,
        risk_factors = EXCLUDED.risk_factors,
        readings_analyzed = EXCLUDED.readings_analyzed,
        heart_rate_trend = EXCLUDED.heart_rate_trend,
        spo2_trend = EXCLUDED.spo2_trend,
        period_start = EXCLUDED.period_start,
        period_end = EXCLUDED.period_end,
        created_at = (NOW() AT TIME ZONE 'UTC')
"""


def build_risk_rows(simulated_day, period_start, period_end, vitals, labs) -> list[tuple]:
    """Pure join + scoring step (no Spark/DB) so it can be unit-tested."""
    rows = []
    for pid in sorted(set(vitals) | set(labs)):
        v = vitals.get(pid, {})
        tests = labs.get(pid, [])
        v_score, v_factors = vitals_risk_score(
            v.get("total", 0), v.get("abnormal", 0) or 0, v.get("hr_trend"), v.get("spo2_trend"),
        )
        l_score, l_factors = lab_risk_score(tests)
        combined, level = combine_risk(v_score, l_score)
        factors = v_factors + l_factors
        rows.append((
            pid, period_end.date(), simulated_day,
            v_score, l_score, combined, level,
            v.get("abnormal", 0) or 0, sum(1 for t in tests if t["is_abnormal"]),
            "; ".join(factors) if factors else "None",
            v.get("total", 0),
            round(v["hr_trend"], 4) if v.get("hr_trend") is not None else None,
            round(v["spo2_trend"], 4) if v.get("spo2_trend") is not None else None,
            period_start, period_end,
        ))
    return rows


def run_daily_risk_processor(simulated_day: int, spark=None) -> int:
    """End-to-end batch run for one simulated day. Called by the Airflow DAG."""
    structlog.contextvars.bind_contextvars(batch_run_id=str(uuid.uuid4())[:8], simulated_day=simulated_day)
    logger.info("batch_processing_started")
    start = time.time()
    own_spark = spark is None
    spark = spark or create_spark_session()

    try:
        lab_count, collected_at = ingest_lab_file(simulated_day)
        period_end = collected_at
        period_start = period_end - timedelta(seconds=SIMULATED_DAY_SECONDS)

        vitals = compute_vitals_features(spark, period_start, period_end)
        readings = sum(v["total"] for v in vitals.values())
        batch_raw_readings_processed_total.inc(readings)
        logger.info("vitals_features_computed", patients=len(vitals), readings=readings,
                    period_start=str(period_start), period_end=str(period_end))

        rows = build_risk_rows(simulated_day, period_start, period_end, vitals, load_lab_results(simulated_day))
        execute_batch(UPSERT_RISK_SQL, rows)
        for r in rows:
            logger.info("patient_risk_computed", patient_id=r[0], vitals_risk=r[3],
                        lab_risk=r[4], combined=r[5], level=r[6])

        batch_risk_reports_written_total.inc(len(rows))
        batch_last_simulated_day.set(simulated_day)
        batch_run_duration_seconds.set(time.time() - start)
        heartbeat("batch_layer")
        push_metrics("batch_layer")
        logger.info("batch_processing_completed", patients=len(rows),
                    lab_records=lab_count, duration_s=round(time.time() - start, 1))
        return len(rows)
    except Exception:
        logger.exception("batch_processing_failed")
        raise
    finally:
        structlog.contextvars.unbind_contextvars("batch_run_id", "simulated_day")
        if own_spark:
            spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the daily batch risk processor")
    parser.add_argument("--day", type=int, required=True, help="Simulated day number")
    args = parser.parse_args()
    print(f"Processed {run_daily_risk_processor(args.day)} patients for simulated day {args.day}")
