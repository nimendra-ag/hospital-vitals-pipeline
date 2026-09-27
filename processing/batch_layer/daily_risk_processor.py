"""
Batch layer — Daily patient risk reconciliation processor.

Runs once per simulated day (triggered by Airflow).  It:
  1. Ingests the day's lab results file into the lab_results table.
  2. Pulls that day's vitals aggregates from PostgreSQL.
  3. Computes a per-patient composite risk score by joining vitals
     trends with lab results.
  4. Writes the results to the patient_risk table.

Risk scoring model (simplified for a mini-project):
  - Vitals risk:  proportion of 30-second windows where any vital
                  was outside the normal range × 100.
  - Lab risk:     proportion of lab tests that returned abnormal × 100.
  - Combined:     0.6 × vitals_risk + 0.4 × lab_risk  (weighted
                  toward vitals because they are more immediately
                  actionable).
  - Risk level:   LOW (<25), MODERATE (25-50), HIGH (50-75),
                  CRITICAL (>75).
"""

import os
import json
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Append project root so imports resolve when Airflow runs the script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from storage.db import execute_query, execute_batch, execute_write
from observability.logging_config import get_logger
from observability.metrics import metrics

logger = get_logger("processing.batch_layer")

LAB_DATA_DIR = os.getenv("LAB_DATA_DIR", "data/lab_results")

# Vitals normal ranges (same as vitals_producer — centralise in production)
VITALS_NORMAL = {
    "heart_rate":   (60, 100),
    "spo2":         (95, 100),
    "systolic_bp":  (90, 140),
    "diastolic_bp": (60, 90),
    "temperature":  (36.1, 37.2),
}


# ── Step 1: Ingest lab results file ───────────────────────────────

def ingest_lab_file(simulated_day: int) -> int:
    """
    Read the lab results JSON file for the given simulated day
    and insert the records into the lab_results table.

    Returns the number of records inserted.
    """
    filename = f"lab_results_day_{str(simulated_day).zfill(3)}.json"
    filepath = Path(LAB_DATA_DIR) / filename

    if not filepath.exists():
        logger.error("lab_file_not_found", path=str(filepath), day=simulated_day)
        raise FileNotFoundError(f"Lab file not found: {filepath}")

    with open(filepath, "r") as f:
        records = json.load(f)

    rows = [
        (
            r["patient_id"],
            r["test_type"],
            r["result_value"],
            r["reference_min"],
            r["reference_max"],
            r["is_abnormal"],
            r["collected_at"],
            r["simulated_day"],
        )
        for r in records
    ]

    count = execute_batch(
        """
        INSERT INTO lab_results
            (patient_id, test_type, result_value, reference_min,
             reference_max, is_abnormal, collected_at, simulated_day)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )

    logger.info("lab_file_ingested", day=simulated_day, records=len(rows))
    metrics.increment("batch_layer", "lab_records_ingested", len(rows))
    return len(rows)


# ── Step 2: Compute vitals risk per patient ───────────────────────

def compute_vitals_risk(simulated_day: int) -> dict[str, dict]:
    """
    Query vitals_aggregates for windows that fall within the current
    simulated day and compute a risk score per patient.

    Returns {patient_id: {"score": float, "abnormal_count": int, "total": int, "factors": [str]}}
    """
    # Approximate the time window for this simulated day.
    # Each simulated day = SIMULATED_DAY_SECONDS of real time.
    # We look at the most recent N hours of vitals data.
    rows = execute_query(
        """
        SELECT
            patient_id,
            avg_heart_rate,
            avg_spo2,
            avg_systolic_bp,
            avg_diastolic_bp,
            avg_temperature,
            min_spo2,
            max_heart_rate
        FROM vitals_aggregates
        WHERE created_at >= NOW() - INTERVAL '10 minutes'
        ORDER BY patient_id, window_start
        """
    )

    patient_stats: dict[str, dict] = {}

    for row in rows:
        pid = row["patient_id"]
        if pid not in patient_stats:
            patient_stats[pid] = {
                "total_windows": 0,
                "abnormal_windows": 0,
                "factors": [],
            }

        stats = patient_stats[pid]
        stats["total_windows"] += 1

        is_abnormal = False
        hr = row["avg_heart_rate"]
        spo2 = row["avg_spo2"]
        sbp = row["avg_systolic_bp"]
        dbp = row["avg_diastolic_bp"]
        temp = row["avg_temperature"]

        if hr and (hr < VITALS_NORMAL["heart_rate"][0] or hr > VITALS_NORMAL["heart_rate"][1]):
            is_abnormal = True
            direction = "high" if hr > VITALS_NORMAL["heart_rate"][1] else "low"
            stats["factors"].append(f"Heart rate {direction} ({hr:.1f})")

        if spo2 and spo2 < VITALS_NORMAL["spo2"][0]:
            is_abnormal = True
            stats["factors"].append(f"SpO2 low ({spo2:.1f})")

        if sbp and (sbp < VITALS_NORMAL["systolic_bp"][0] or sbp > VITALS_NORMAL["systolic_bp"][1]):
            is_abnormal = True
            direction = "high" if sbp > VITALS_NORMAL["systolic_bp"][1] else "low"
            stats["factors"].append(f"Systolic BP {direction} ({sbp:.1f})")

        if temp and (temp < VITALS_NORMAL["temperature"][0] or temp > VITALS_NORMAL["temperature"][1]):
            is_abnormal = True
            direction = "high" if temp > VITALS_NORMAL["temperature"][1] else "low"
            stats["factors"].append(f"Temperature {direction} ({temp:.2f})")

        if is_abnormal:
            stats["abnormal_windows"] += 1

    # Compute scores
    result = {}
    for pid, stats in patient_stats.items():
        total = stats["total_windows"]
        abnormal = stats["abnormal_windows"]
        score = (abnormal / total * 100) if total > 0 else 0.0

        # Deduplicate factors
        unique_factors = list(set(stats["factors"]))

        result[pid] = {
            "score": round(score, 2),
            "abnormal_count": abnormal,
            "total": total,
            "factors": unique_factors[:10],  # cap at 10 most recent
        }

    return result


# ── Step 3: Compute lab risk per patient ──────────────────────────

def compute_lab_risk(simulated_day: int) -> dict[str, dict]:
    """
    Query lab_results for the given simulated day and compute a
    risk score per patient.

    Returns {patient_id: {"score": float, "abnormal_count": int, "total": int, "factors": [str]}}
    """
    rows = execute_query(
        """
        SELECT patient_id, test_type, result_value,
               reference_min, reference_max, is_abnormal
        FROM lab_results
        WHERE simulated_day = %s
        """,
        (simulated_day,),
    )

    patient_stats: dict[str, dict] = {}

    for row in rows:
        pid = row["patient_id"]
        if pid not in patient_stats:
            patient_stats[pid] = {
                "total_tests": 0,
                "abnormal_tests": 0,
                "factors": [],
            }

        stats = patient_stats[pid]
        stats["total_tests"] += 1

        if row["is_abnormal"]:
            stats["abnormal_tests"] += 1
            direction = "high" if row["result_value"] > row["reference_max"] else "low"
            stats["factors"].append(
                f"{row['test_type']} {direction} ({row['result_value']:.2f}, "
                f"ref: {row['reference_min']}-{row['reference_max']})"
            )

    result = {}
    for pid, stats in patient_stats.items():
        total = stats["total_tests"]
        abnormal = stats["abnormal_tests"]
        score = (abnormal / total * 100) if total > 0 else 0.0
        result[pid] = {
            "score": round(score, 2),
            "abnormal_count": abnormal,
            "total": total,
            "factors": stats["factors"],
        }

    return result


# ── Step 4: Combine and write risk report ─────────────────────────

def classify_risk(score: float) -> str:
    """Map a numeric risk score to a categorical level."""
    if score >= 75:
        return "CRITICAL"
    elif score >= 50:
        return "HIGH"
    elif score >= 25:
        return "MODERATE"
    return "LOW"


def run_daily_risk_processor(simulated_day: int):
    """
    End-to-end daily batch processing pipeline.

    This is the function called by the Airflow DAG.
    """
    logger.info("batch_processing_started", simulated_day=simulated_day)

    # Step 1 — Ingest lab file
    lab_count = ingest_lab_file(simulated_day)

    # Step 2 — Compute vitals risk
    vitals_risk = compute_vitals_risk(simulated_day)

    # Step 3 — Compute lab risk
    lab_risk = compute_lab_risk(simulated_day)

    # Step 4 — Combine and persist
    all_patients = set(list(vitals_risk.keys()) + list(lab_risk.keys()))
    report_date = date.today()

    rows = []
    for pid in sorted(all_patients):
        v = vitals_risk.get(pid, {"score": 0, "abnormal_count": 0, "factors": []})
        l = lab_risk.get(pid, {"score": 0, "abnormal_count": 0, "factors": []})

        combined = round(0.6 * v["score"] + 0.4 * l["score"], 2)
        risk_level = classify_risk(combined)

        all_factors = v["factors"] + l["factors"]
        factors_str = "; ".join(all_factors) if all_factors else "None"

        rows.append((
            pid,
            report_date,
            simulated_day,
            v["score"],
            l["score"],
            combined,
            risk_level,
            v["abnormal_count"],
            l["abnormal_count"],
            factors_str,
        ))

        logger.info(
            "patient_risk_computed",
            patient_id=pid,
            vitals_risk=v["score"],
            lab_risk=l["score"],
            combined=combined,
            level=risk_level,
        )

    execute_batch(
        """
        INSERT INTO patient_risk
            (patient_id, report_date, simulated_day, vitals_risk_score,
             lab_risk_score, combined_risk_score, risk_level,
             abnormal_vitals_count, abnormal_labs_count, risk_factors)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patient_id, report_date)
        DO UPDATE SET
            simulated_day = EXCLUDED.simulated_day,
            vitals_risk_score = EXCLUDED.vitals_risk_score,
            lab_risk_score = EXCLUDED.lab_risk_score,
            combined_risk_score = EXCLUDED.combined_risk_score,
            risk_level = EXCLUDED.risk_level,
            abnormal_vitals_count = EXCLUDED.abnormal_vitals_count,
            abnormal_labs_count = EXCLUDED.abnormal_labs_count,
            risk_factors = EXCLUDED.risk_factors,
            created_at = NOW()
        """,
        rows,
    )

    metrics.increment("batch_layer", "risk_reports_written", len(rows))
    logger.info(
        "batch_processing_completed",
        simulated_day=simulated_day,
        patients_processed=len(rows),
        lab_records_ingested=lab_count,
    )

    return len(rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run daily batch risk processor")
    parser.add_argument("--day", type=int, required=True, help="Simulated day number")
    args = parser.parse_args()

    count = run_daily_risk_processor(args.day)
    print(f"Processed {count} patients for simulated day {args.day}")
