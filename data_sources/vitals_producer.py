"""
Streaming data source — Bedside vital-sign simulator.

Simulates bedside monitors for N patients, each emitting a JSON
reading every few seconds to the Kafka topic `patient-vitals`.

Partitioning:  Messages are keyed by patient_id so Kafka routes all
readings for a single patient to the same partition — a prerequisite
for per-patient stateful aggregation in the speed layer.

Abnormal spikes:  ~8 % of readings are deliberately pushed outside
normal clinical ranges to exercise the alerting path.

Simulated-time clock:  The producer emits real-world timestamps
(not compressed).  Time compression happens only in how often the
batch source drops a new lab file (1 simulated day = SIMULATED_DAY_SECONDS).
"""

import os
import json
import time
import random
import signal
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

from observability.logging_config import get_logger
from observability.metrics import metrics

load_dotenv()
logger = get_logger("data_sources.vitals_producer")

# ── Configuration ──────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VITALS_TOPIC = os.getenv("KAFKA_VITALS_TOPIC", "patient-vitals")
NUM_PATIENTS = int(os.getenv("NUM_PATIENTS", "12"))
EMIT_INTERVAL = float(os.getenv("VITALS_EMIT_INTERVAL_SECONDS", "3"))

# ── Clinical reference ranges ─────────────────────────────────────
NORMAL_RANGES = {
    "heart_rate":   {"min": 60,  "max": 100},
    "spo2":         {"min": 95,  "max": 100},
    "systolic_bp":  {"min": 90,  "max": 140},
    "diastolic_bp": {"min": 60,  "max": 90},
    "temperature":  {"min": 36.1, "max": 37.2},
}

ABNORMAL_PROBABILITY = 0.08  # 8 % chance of an abnormal spike


class PatientSimulator:
    """
    Maintains per-patient state so that consecutive readings are
    physiologically plausible (small random walk rather than fully
    independent draws).
    """

    def __init__(self, patient_id: str):
        self.patient_id = patient_id
        # Start each patient near the center of normal ranges
        self.heart_rate = random.uniform(68, 82)
        self.spo2 = random.uniform(96, 99)
        self.systolic_bp = random.uniform(110, 130)
        self.diastolic_bp = random.uniform(65, 80)
        self.temperature = random.uniform(36.3, 36.9)

    def _walk(self, current: float, low: float, high: float, step: float) -> float:
        """Random-walk the value within [low, high]."""
        delta = random.gauss(0, step)
        return max(low, min(high, current + delta))

    def generate_reading(self) -> dict:
        """
        Produce the next vital-sign reading.

        With probability ABNORMAL_PROBABILITY the reading is an
        intentional spike outside the normal range.
        """
        is_abnormal = random.random() < ABNORMAL_PROBABILITY

        if is_abnormal:
            # Pick a random vital to spike
            spike_vital = random.choice(
                ["heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"]
            )
            self._apply_spike(spike_vital)
        else:
            # Normal random walk
            self.heart_rate = self._walk(self.heart_rate, 55, 105, 1.5)
            self.spo2 = self._walk(self.spo2, 94, 100, 0.3)
            self.systolic_bp = self._walk(self.systolic_bp, 85, 145, 2.0)
            self.diastolic_bp = self._walk(self.diastolic_bp, 55, 95, 1.5)
            self.temperature = self._walk(self.temperature, 36.0, 37.4, 0.05)

        return {
            "patient_id": self.patient_id,
            "heart_rate": round(self.heart_rate, 1),
            "spo2": round(self.spo2, 1),
            "systolic_bp": round(self.systolic_bp, 1),
            "diastolic_bp": round(self.diastolic_bp, 1),
            "temperature": round(self.temperature, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_abnormal": is_abnormal,
        }

    def _apply_spike(self, vital: str):
        """Push one vital outside the normal range."""
        if vital == "heart_rate":
            self.heart_rate = random.choice([
                random.uniform(120, 160),   # tachycardia
                random.uniform(35, 50),     # bradycardia
            ])
        elif vital == "spo2":
            self.spo2 = random.uniform(80, 89)  # hypoxemia
        elif vital == "systolic_bp":
            self.systolic_bp = random.choice([
                random.uniform(180, 220),   # hypertensive crisis
                random.uniform(60, 80),     # hypotension
            ])
        elif vital == "diastolic_bp":
            self.diastolic_bp = random.choice([
                random.uniform(95, 120),
                random.uniform(40, 55),
            ])
        elif vital == "temperature":
            self.temperature = random.choice([
                random.uniform(38.5, 40.5), # fever
                random.uniform(34.0, 35.5), # hypothermia
            ])


def delivery_callback(err, msg):
    """Kafka producer delivery report callback."""
    if err is not None:
        metrics.increment("vitals_producer", "send_errors")
        logger.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
        )
    else:
        metrics.increment("vitals_producer", "records_sent")


def run_producer():
    """Main producer loop — runs until SIGINT / SIGTERM."""
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "linger.ms": 50,
        "batch.num.messages": 20,
        "compression.type": "snappy",
    })

    patients = [
        PatientSimulator(f"P{str(i).zfill(3)}")
        for i in range(1, NUM_PATIENTS + 1)
    ]

    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True
        logger.info("shutdown_signal_received", signal=signum)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(
        "vitals_producer_started",
        num_patients=NUM_PATIENTS,
        emit_interval=EMIT_INTERVAL,
        topic=VITALS_TOPIC,
    )

    cycle = 0
    while not shutdown:
        cycle += 1
        for patient in patients:
            reading = patient.generate_reading()

            producer.produce(
                topic=VITALS_TOPIC,
                key=reading["patient_id"].encode("utf-8"),
                value=json.dumps(reading).encode("utf-8"),
                callback=delivery_callback,
            )

            if reading["is_abnormal"]:
                logger.warning(
                    "abnormal_reading_emitted",
                    patient_id=reading["patient_id"],
                    heart_rate=reading["heart_rate"],
                    spo2=reading["spo2"],
                    temperature=reading["temperature"],
                )

        # Flush after each batch of patient readings
        producer.poll(0)

        if cycle % 10 == 0:
            sent = metrics.get_counter("vitals_producer", "records_sent")
            errors = metrics.get_counter("vitals_producer", "send_errors")
            logger.info(
                "producer_heartbeat",
                cycle=cycle,
                total_sent=sent,
                total_errors=errors,
            )

        time.sleep(EMIT_INTERVAL)

    # Graceful shutdown — flush remaining messages
    remaining = producer.flush(timeout=10)
    logger.info("vitals_producer_stopped", unflushed=remaining)


if __name__ == "__main__":
    from ingestion.kafka_config import setup_kafka
    setup_kafka()
    run_producer()
