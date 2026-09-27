"""
Airflow DAG — Daily patient risk pipeline.

Orchestrates the batch layer of the Lambda architecture:
  1. Sense whether a new lab results file has arrived.
  2. Ingest the lab file into PostgreSQL.
  3. Run the batch risk processor (join vitals + labs → risk scores).
  4. Generate the daily HTML/JSON risk report.

Schedule:
  Runs every SIMULATED_DAY_SECONDS (default 5 min) to match the
  simulated-time compression.  In production this would be a daily
  schedule (e.g. 02:00 UTC).

The DAG uses a shared state file (data/.current_day) to track the
simulated day counter across runs, so that each execution processes
the correct lab file.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from dotenv import load_dotenv

# Load project env and ensure project root is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

SIMULATED_DAY_SECONDS = int(os.getenv("SIMULATED_DAY_SECONDS", "300"))
LAB_DATA_DIR = os.getenv("LAB_DATA_DIR", "data/lab_results")

# ── Helper: simulated day counter ─────────────────────────────────

DAY_COUNTER_FILE = os.path.join(PROJECT_ROOT, "data", ".current_day")


def _read_day() -> int:
    """Read the current simulated day from a state file."""
    path = Path(DAY_COUNTER_FILE)
    if path.exists():
        return int(path.read_text().strip())
    return 0


def _write_day(day: int):
    """Persist the current simulated day."""
    path = Path(DAY_COUNTER_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(day))


# ── Task callables ────────────────────────────────────────────────

def check_lab_file(**context):
    """
    ShortCircuit: return True only if the expected lab file exists.
    Pushes the simulated_day via XCom for downstream tasks.
    """
    current_day = _read_day() + 1
    filename = f"lab_results_day_{str(current_day).zfill(3)}.json"
    filepath = os.path.join(PROJECT_ROOT, LAB_DATA_DIR, filename)

    exists = os.path.isfile(filepath)
    if exists:
        context["ti"].xcom_push(key="simulated_day", value=current_day)
        context["ti"].xcom_push(key="lab_filepath", value=filepath)
        print(f"Lab file found for day {current_day}: {filepath}")
    else:
        print(f"Lab file not yet available for day {current_day}: {filepath}")

    return exists  # ShortCircuitOperator skips downstream if False


def ingest_lab_results(**context):
    """Ingest the lab results file into PostgreSQL."""
    from processing.batch_layer.daily_risk_processor import ingest_lab_file

    day = context["ti"].xcom_pull(key="simulated_day")
    count = ingest_lab_file(day)
    print(f"Ingested {count} lab records for day {day}")


def run_batch_risk_processor(**context):
    """
    Compute per-patient risk scores by joining vitals aggregates
    with the day's lab results.
    """
    from processing.batch_layer.daily_risk_processor import (
        compute_vitals_risk,
        compute_lab_risk,
        classify_risk,
    )
    from storage.db import execute_batch

    day = context["ti"].xcom_pull(key="simulated_day")
    from datetime import date as dt_date

    vitals_risk = compute_vitals_risk(day)
    lab_risk = compute_lab_risk(day)

    all_patients = set(list(vitals_risk.keys()) + list(lab_risk.keys()))
    report_date = dt_date.today()
    rows = []

    for pid in sorted(all_patients):
        v = vitals_risk.get(pid, {"score": 0, "abnormal_count": 0, "factors": []})
        l = lab_risk.get(pid, {"score": 0, "abnormal_count": 0, "factors": []})

        combined = round(0.6 * v["score"] + 0.4 * l["score"], 2)
        risk_level = classify_risk(combined)
        all_factors = v.get("factors", []) + l.get("factors", [])
        factors_str = "; ".join(all_factors) if all_factors else "None"

        rows.append((
            pid, report_date, day,
            v["score"], l["score"], combined, risk_level,
            v.get("abnormal_count", 0), l.get("abnormal_count", 0),
            factors_str,
        ))

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
    print(f"Computed risk scores for {len(rows)} patients (day {day})")


def generate_report(**context):
    """Generate HTML and JSON risk report files."""
    from serving.report_generator import generate_report as gen_report
    from datetime import date as dt_date

    day = context["ti"].xcom_pull(key="simulated_day")
    report = gen_report(report_date=dt_date.today(), simulated_day=day)
    print(f"Report generated: {report.get('total_patients', 0)} patients")


def advance_day_counter(**context):
    """Increment the simulated day counter after successful processing."""
    day = context["ti"].xcom_pull(key="simulated_day")
    _write_day(day)
    print(f"Day counter advanced to {day}")


# ── DAG definition ────────────────────────────────────────────────

default_args = {
    "owner": "hospital-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_patient_risk_pipeline",
    default_args=default_args,
    description="Daily batch pipeline: ingest lab results → compute risk → generate report",
    schedule_interval=timedelta(seconds=SIMULATED_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hospital", "batch", "lambda"],
) as dag:

    t_check = ShortCircuitOperator(
        task_id="check_lab_file",
        python_callable=check_lab_file,
        provide_context=True,
    )

    t_ingest = PythonOperator(
        task_id="ingest_lab_results",
        python_callable=ingest_lab_results,
        provide_context=True,
    )

    t_risk = PythonOperator(
        task_id="compute_risk_scores",
        python_callable=run_batch_risk_processor,
        provide_context=True,
    )

    t_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        provide_context=True,
    )

    t_advance = PythonOperator(
        task_id="advance_day_counter",
        python_callable=advance_day_counter,
        provide_context=True,
    )

    # Task dependencies — linear pipeline
    t_check >> t_ingest >> t_risk >> t_report >> t_advance
