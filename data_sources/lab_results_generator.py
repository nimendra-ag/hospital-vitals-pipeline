"""
Batch data source — Daily lab results generator.

Simulates a pathology lab that produces one JSON file per simulated day
containing ONE lab report per patient. Each report is the patient's daily
blood panel: all five tests (WBC, Hemoglobin, Glucose, Creatinine, CRP),
each with the spec's fields (test_type, result_value, reference_range,
collected_at).

Time compression:
    1 simulated day = SIMULATED_DAY_SECONDS (default 300 s = 5 min).
    The generator emits a new file every SIMULATED_DAY_SECONDS,
    incrementing the simulated_day counter.

Output directory:
    data/lab_results/lab_results_day_NNN.json

    On restart the generator resumes after the highest day already on disk
    (it never overwrites an earlier day's file), and each file is written
    to a temp name then atomically renamed, so the Airflow sensor can never
    pick up a half-written file.

    Each file is an end-of-day extract: collected_at is the moment the day
    closes, and the batch layer pairs it with the SIMULATED_DAY_SECONDS of
    vitals that precede it.

File schema (one JSON array, one report per patient):
    [
      {
        "report_id": "LAB-D001-P001",
        "patient_id": "P001",
        "simulated_day": 1,
        "collected_at": "2026-09-30T15:08:41+00:00",
        "tests": [
          {"test_type": "WBC", "result_value": 7.2, "unit": "10^3/uL",
           "reference_min": 4.0, "reference_max": 11.0,
           "reference_range": "4.0-11.0", "is_abnormal": false},
          ... (Hemoglobin, Glucose, Creatinine, CRP)
        ]
      },
      ...
    ]
"""

import os
import re
import json
import time
import random
import signal
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

from observability.logging_config import get_logger
from observability.metrics import metrics
from observability.prometheus_metrics import (
    lab_files_generated_total,
    lab_records_generated_total,
    heartbeat,
    push as push_metrics,
)

load_dotenv()
logger = get_logger("data_sources.lab_results_generator")

# ── Configuration ──────────────────────────────────────────────────
NUM_PATIENTS = int(os.getenv("NUM_PATIENTS", "12"))
SIMULATED_DAY_SECONDS = int(os.getenv("SIMULATED_DAY_SECONDS", "300"))
LAB_DATA_DIR = os.getenv("LAB_DATA_DIR", "data/lab_results")

# ── Daily blood panel (every report contains all of these) ────────
# Each test: (test_type, unit, ref_min, ref_max, normal_mean, normal_std)
LAB_TESTS = [
    ("WBC",         "10^3/uL", 4.0,  11.0, 7.5,  1.5),
    ("Hemoglobin",  "g/dL",    12.0, 17.5, 14.5, 1.2),
    ("Glucose",     "mg/dL",   70,   100,  85,   8),
    ("Creatinine",  "mg/dL",   0.6,  1.2,  0.9,  0.15),
    ("CRP",         "mg/L",    0.0,  5.0,  2.0,  1.5),
]

ABNORMAL_PROBABILITY = 0.12  # each test is out of range ~12 % of the time


def _test_result(test: tuple) -> dict:
    """One test result inside a report."""
    test_type, unit, ref_min, ref_max, mean, std = test
    if random.random() < ABNORMAL_PROBABILITY:
        # Push the value outside the reference range. Tests whose lower
        # bound is 0 (e.g. CRP) can only be abnormally HIGH: a negative
        # concentration is physically impossible.
        if ref_min > 0 and random.random() < 0.5:
            value = ref_min - random.uniform(0.05, 0.3) * ref_min
        else:
            value = ref_max + random.uniform(0.05, 0.6) * ref_max
    else:
        value = max(ref_min, min(ref_max, random.gauss(mean, std)))  # stays in range
    return {
        "test_type": test_type,
        "result_value": round(value, 2),
        "unit": unit,
        "reference_min": ref_min,
        "reference_max": ref_max,
        "reference_range": f"{ref_min}-{ref_max}",
        "is_abnormal": bool(value < ref_min or value > ref_max),
    }


def generate_lab_report(patient_id: str, simulated_day: int) -> dict:
    """One patient's daily lab report: the full panel of LAB_TESTS."""
    return {
        "report_id": f"LAB-D{simulated_day:03d}-{patient_id}",
        "patient_id": patient_id,
        "simulated_day": simulated_day,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "tests": [_test_result(t) for t in LAB_TESTS],
    }


def generate_daily_file(simulated_day: int) -> str:
    """
    Generate the lab results file for one simulated day.

    Returns the path of the created file.
    """
    all_results = [
        generate_lab_report(f"P{str(i).zfill(3)}", simulated_day)
        for i in range(1, NUM_PATIENTS + 1)
    ]

    # Ensure output directory exists
    output_dir = Path(LAB_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"lab_results_day_{str(simulated_day).zfill(3)}.json"
    filepath = output_dir / filename

    # Atomic publish: the DAG only ever sees complete files.
    tmp_path = filepath.with_suffix(".json.tmp")
    with open(tmp_path, "w") as f:
        json.dump(all_results, f, indent=2)
    os.replace(tmp_path, filepath)

    abnormal_count = sum(t["is_abnormal"] for r in all_results for t in r["tests"])

    logger.info(
        "lab_file_generated",
        simulated_day=simulated_day,
        filepath=str(filepath),
        reports=len(all_results),
        total_tests=len(all_results) * len(LAB_TESTS),
        abnormal_tests=abnormal_count,
    )
    metrics.increment("lab_generator", "files_generated")
    metrics.increment("lab_generator", "records_generated", len(all_results))
    lab_files_generated_total.inc()
    lab_records_generated_total.inc(len(all_results) * len(LAB_TESTS))
    heartbeat("lab_generator")
    push_metrics("lab_generator")

    return str(filepath)


FILE_PATTERN = re.compile(r"lab_results_day_(\d+)\.json$")


def next_simulated_day(output_dir: str = None) -> int:
    """Return 1 + the highest day already written (1 if none)."""
    directory = Path(output_dir or LAB_DATA_DIR)
    if not directory.exists():
        return 1
    days = [
        int(m.group(1))
        for p in directory.iterdir()
        if (m := FILE_PATTERN.match(p.name))
    ]
    return max(days, default=0) + 1


def run_generator():
    """
    Main loop — generates a new lab file every SIMULATED_DAY_SECONDS.

    Runs until SIGINT / SIGTERM.
    """
    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True
        logger.info("shutdown_signal_received", signal=signum)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(
        "lab_generator_started",
        num_patients=NUM_PATIENTS,
        simulated_day_seconds=SIMULATED_DAY_SECONDS,
        output_dir=LAB_DATA_DIR,
    )

    simulated_day = next_simulated_day()
    logger.info("lab_generator_resuming", first_day=simulated_day)

    # The first file closes a full simulated day, so wait one day before it.
    while not shutdown:
        for _ in range(SIMULATED_DAY_SECONDS):
            if shutdown:
                break
            time.sleep(1)
        if shutdown:
            break

        filepath = generate_daily_file(simulated_day)
        print(f"[Day {simulated_day}] Lab file written -> {filepath}")
        simulated_day += 1

    logger.info("lab_generator_stopped", last_day=simulated_day - 1)


def _parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Daily lab results generator")
    parser.add_argument("--once", action="store_true",
                        help="Write the next day's file immediately and exit (demo/testing)")
    return parser.parse_args()


if __name__ == "__main__":
    if _parse_args().once:
        print(generate_daily_file(next_simulated_day()))
    else:
        run_generator()
