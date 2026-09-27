"""
Batch data source — Daily lab results generator.

Simulates a pathology lab that produces one JSON file per simulated
day containing lab test results for every patient.

Time compression:
    1 simulated day = SIMULATED_DAY_SECONDS (default 300 s = 5 min).
    The generator emits a new file every SIMULATED_DAY_SECONDS,
    incrementing the simulated_day counter.

Output directory:
    data/lab_results/lab_results_day_NNN.json

File schema (one JSON array of records):
    [
      {
        "patient_id": "P001",
        "test_type": "WBC",
        "result_value": 7.2,
        "reference_min": 4.0,
        "reference_max": 11.0,
        "is_abnormal": false,
        "collected_at": "2026-09-27T08:00:00Z",
        "simulated_day": 1
      },
      ...
    ]
"""

import os
import json
import time
import random
import signal
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

from observability.logging_config import get_logger
from observability.metrics import metrics

load_dotenv()
logger = get_logger("data_sources.lab_results_generator")

# ── Configuration ──────────────────────────────────────────────────
NUM_PATIENTS = int(os.getenv("NUM_PATIENTS", "12"))
SIMULATED_DAY_SECONDS = int(os.getenv("SIMULATED_DAY_SECONDS", "300"))
LAB_DATA_DIR = os.getenv("LAB_DATA_DIR", "data/lab_results")

# ── Lab test reference ranges ─────────────────────────────────────
# Each test: (test_type, unit, ref_min, ref_max, normal_mean, normal_std)
LAB_TESTS = [
    ("WBC",         "10^3/uL", 4.0,  11.0, 7.5,  1.5),
    ("Hemoglobin",  "g/dL",    12.0, 17.5, 14.5, 1.2),
    ("Glucose",     "mg/dL",   70,   100,  85,   8),
    ("Creatinine",  "mg/dL",   0.6,  1.2,  0.9,  0.15),
    ("CRP",         "mg/L",    0.0,  5.0,  2.0,  1.5),
]

ABNORMAL_PROBABILITY = 0.15  # 15 % of results are out of range


def generate_lab_result(patient_id: str, simulated_day: int) -> list[dict]:
    """Generate one day's lab results for a single patient."""
    results = []
    collected_at = datetime.now(timezone.utc).isoformat()

    for test_type, unit, ref_min, ref_max, mean, std in LAB_TESTS:
        is_abnormal = random.random() < ABNORMAL_PROBABILITY

        if is_abnormal:
            # Push the value outside the reference range
            if random.random() < 0.5:
                value = ref_min - random.uniform(0.5, ref_min * 0.3)
            else:
                value = ref_max + random.uniform(0.5, ref_max * 0.2)
        else:
            value = random.gauss(mean, std)
            value = max(ref_min * 0.8, min(ref_max * 1.1, value))

        results.append({
            "patient_id": patient_id,
            "test_type": test_type,
            "result_value": round(value, 2),
            "reference_min": ref_min,
            "reference_max": ref_max,
            "is_abnormal": bool(value < ref_min or value > ref_max),
            "collected_at": collected_at,
            "simulated_day": simulated_day,
        })

    return results


def generate_daily_file(simulated_day: int) -> str:
    """
    Generate the lab results file for one simulated day.

    Returns the path of the created file.
    """
    all_results = []
    for i in range(1, NUM_PATIENTS + 1):
        patient_id = f"P{str(i).zfill(3)}"
        all_results.extend(generate_lab_result(patient_id, simulated_day))

    # Ensure output directory exists
    output_dir = Path(LAB_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"lab_results_day_{str(simulated_day).zfill(3)}.json"
    filepath = output_dir / filename

    with open(filepath, "w") as f:
        json.dump(all_results, f, indent=2)

    abnormal_count = sum(1 for r in all_results if r["is_abnormal"])

    logger.info(
        "lab_file_generated",
        simulated_day=simulated_day,
        filepath=str(filepath),
        total_records=len(all_results),
        abnormal_records=abnormal_count,
    )
    metrics.increment("lab_generator", "files_generated")
    metrics.increment("lab_generator", "records_generated", len(all_results))

    return str(filepath)


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

    simulated_day = 1
    while not shutdown:
        filepath = generate_daily_file(simulated_day)
        print(f"[Day {simulated_day}] Lab file written → {filepath}")
        simulated_day += 1

        # Wait for the next simulated day
        for _ in range(SIMULATED_DAY_SECONDS):
            if shutdown:
                break
            time.sleep(1)

    logger.info("lab_generator_stopped", last_day=simulated_day - 1)


if __name__ == "__main__":
    run_generator()
