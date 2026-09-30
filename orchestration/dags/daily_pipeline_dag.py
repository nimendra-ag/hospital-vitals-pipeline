"""
Airflow DAG — daily patient risk pipeline (the Lambda batch layer).

    find_pending_days ──► ingest_lab_results ──► compute_risk_scores ──► generate_reports
      (ShortCircuit)        (idempotent upsert)     (PySpark batch job      (HTML + JSON
                                                     over the raw master      report files)
                                                     dataset + lab join)

Which days to process is derived from state, not a counter file:
    pending = {lab files on disk} - {simulated days already in patient_risk}
so a restarted generator, a failed run or a paused DAG can never desync
the two — the next run simply catches up (up to MAX_DAYS_PER_RUN days).

Schedule: every SIMULATED_DAY_SECONDS / 2 (150 s by default) so a new lab
file (one per 300 s simulated day) is picked up within half a simulated
day. In production this would be a daily cron (e.g. 02:00 UTC).
"""

import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

SIMULATED_DAY_SECONDS = int(os.getenv("SIMULATED_DAY_SECONDS", "300"))
LAB_DATA_DIR = os.path.join(PROJECT_ROOT, os.getenv("LAB_DATA_DIR", "data/lab_results"))
MAX_DAYS_PER_RUN = int(os.getenv("BATCH_MAX_DAYS_PER_RUN", "3"))
FILE_PATTERN = re.compile(r"lab_results_day_(\d+)\.json$")


def find_pending_days(**context) -> bool:
    """ShortCircuit: continue only if there are lab files not yet processed."""
    from storage.db import execute_query

    on_disk = sorted(
        int(m.group(1)) for p in Path(LAB_DATA_DIR).glob("lab_results_day_*.json")
        if (m := FILE_PATTERN.match(p.name))
    )
    done = {r["simulated_day"] for r in execute_query("SELECT DISTINCT simulated_day FROM patient_risk")}
    pending = [d for d in on_disk if d not in done][:MAX_DAYS_PER_RUN]

    print(f"Lab files on disk: {len(on_disk)}, processed: {len(done)}, this run: {pending}")
    context["ti"].xcom_push(key="pending_days", value=pending)
    return bool(pending)


def ingest_lab_results(**context):
    from processing.batch_layer.daily_risk_processor import ingest_lab_file

    for day in context["ti"].xcom_pull(key="pending_days"):
        count, collected_at = ingest_lab_file(day)
        print(f"Day {day}: ingested {count} lab records (collected_at {collected_at})")


def compute_risk_scores(**context):
    """One SparkSession for all pending days (JVM start-up dominates cost)."""
    from processing.batch_layer.daily_risk_processor import create_spark_session, run_daily_risk_processor

    spark = create_spark_session()
    try:
        for day in context["ti"].xcom_pull(key="pending_days"):
            print(f"Day {day}: risk computed for {run_daily_risk_processor(day, spark=spark)} patients")
    finally:
        spark.stop()


def generate_reports(**context):
    from serving.report_generator import generate_report

    for day in context["ti"].xcom_pull(key="pending_days"):
        report = generate_report(simulated_day=day)
        print(f"Day {day}: report for {report.get('total_patients', 0)} patients")


default_args = {
    "owner": "hospital-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_patient_risk_pipeline",
    default_args=default_args,
    description="Lambda batch layer: ingest daily lab file -> Spark recompute from master dataset -> risk report",
    schedule=timedelta(seconds=max(SIMULATED_DAY_SECONDS // 2, 60)),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hospital", "batch", "lambda"],
) as dag:
    t_find = ShortCircuitOperator(task_id="find_pending_days", python_callable=find_pending_days)
    t_ingest = PythonOperator(task_id="ingest_lab_results", python_callable=ingest_lab_results)
    t_risk = PythonOperator(task_id="compute_risk_scores", python_callable=compute_risk_scores)
    t_report = PythonOperator(task_id="generate_reports", python_callable=generate_reports)

    t_find >> t_ingest >> t_risk >> t_report
