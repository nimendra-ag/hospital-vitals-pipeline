"""
Streaming data source — Bedside vital-sign simulator.

Simulates bedside monitors for N patients, each emitting a JSON
reading every few seconds to the Kafka topic `patient-vitals`.

Partitioning:  Messages are keyed by patient_id so Kafka routes all
readings for a single patient to the same partition — a prerequisite
for per-patient stateful aggregation in the speed layer.

Patient behaviour (per patient, per reading):
  - stable:        vitals mean-revert around the patient's own baseline
                   with small noise.
  - deteriorating: occasionally (EPISODE_PROBABILITY) a patient starts an
                   episode — HR climbs, SpO2 and BP fall, temperature rises
                   gradually over a few minutes (sepsis-like picture). This
                   produces the sustained, *trending* breaches the business
                   question is about.
  - recovering:    after the episode, vitals drift back to baseline.
  - spikes:        rarely (SPIKE_PROBABILITY, 1 %) a single reading is a
                   transient artefact (motion, probe displacement) outside
                   the normal range; it does not change the patient's state.

Tracing:  every reading carries a unique event_id. It is logged here for
abnormal readings, stored on any alert the speed layer raises from it, and
kept in the raw master dataset, so one reading can be followed end to end.

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
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

from observability.logging_config import get_logger
from observability.metrics import metrics
from observability.prometheus_metrics import (
    vitals_records_sent_total,
    vitals_send_errors_total,
    vitals_abnormal_emitted_total,
    heartbeat,
    push as push_metrics,
)

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

SPIKE_PROBABILITY = float(os.getenv("VITALS_SPIKE_PROBABILITY", "0.01"))
EPISODE_PROBABILITY = float(os.getenv("VITALS_EPISODE_PROBABILITY", "0.0015"))
EPISODE_READINGS = (60, 110)   # 3-5.5 min of worsening at a 3 s interval
PULL = 0.04                    # mean-reversion strength per reading

# Where a deteriorating patient's vitals head during an episode.
DETERIORATED = {"heart_rate": 132.0, "spo2": 87.5, "systolic_bp": 86.0,
                "diastolic_bp": 55.0, "temperature": 38.9}
NOISE = {"heart_rate": 1.5, "spo2": 0.3, "systolic_bp": 2.0,
         "diastolic_bp": 1.5, "temperature": 0.04}
VITALS = list(NOISE)


class PatientSimulator:
    """
    Per-patient state machine (stable -> deteriorating -> recovering ->
    stable) with mean-reverting noise, so consecutive readings are
    physiologically plausible and trends are real, not random walk noise.
    """

    def __init__(self, patient_id: str):
        self.patient_id = patient_id
        self.baseline = {
            "heart_rate": random.uniform(68, 82),
            "spo2": random.uniform(96.5, 98.5),
            "systolic_bp": random.uniform(110, 130),
            "diastolic_bp": random.uniform(65, 80),
            "temperature": random.uniform(36.4, 36.9),
        }
        self.values = dict(self.baseline)
        self.condition = "stable"
        self._episode_left = 0

    def start_episode(self, readings: int | None = None):
        """Begin a deterioration episode (also used by tests / demos)."""
        self.condition = "deteriorating"
        self._episode_left = readings or random.randint(*EPISODE_READINGS)

    def _advance_state(self):
        if self.condition == "stable" and random.random() < EPISODE_PROBABILITY:
            self.start_episode()
        elif self.condition == "deteriorating":
            self._episode_left -= 1
            if self._episode_left <= 0:
                self.condition = "recovering"
        elif self.condition == "recovering":
            if abs(self.values["heart_rate"] - self.baseline["heart_rate"]) < 4                     and abs(self.values["spo2"] - self.baseline["spo2"]) < 0.8:
                self.condition = "stable"

    def generate_reading(self) -> dict:
        """Produce the next vital-sign reading."""
        self._advance_state()
        target = DETERIORATED if self.condition == "deteriorating" else self.baseline
        for v in VITALS:
            self.values[v] += PULL * (target[v] - self.values[v]) + random.gauss(0, NOISE[v])
        self.values["spo2"] = min(self.values["spo2"], 100.0)

        reading = dict(self.values)
        is_spike = random.random() < SPIKE_PROBABILITY
        if is_spike:
            vital = random.choice(VITALS)
            reading[vital] = self._spike_value(vital)

        return {
            "event_id": str(uuid.uuid4()),
            "patient_id": self.patient_id,
            "heart_rate": round(reading["heart_rate"], 1),
            "spo2": round(reading["spo2"], 1),
            "systolic_bp": round(reading["systolic_bp"], 1),
            "diastolic_bp": round(reading["diastolic_bp"], 1),
            "temperature": round(reading["temperature"], 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_abnormal": is_spike,
            "condition": self.condition,
        }

    @staticmethod
    def _spike_value(vital: str) -> float:
        """A single transient out-of-range value (the state is not changed)."""
        return {
            "heart_rate": lambda: random.choice([random.uniform(125, 160), random.uniform(35, 48)]),
            "spo2": lambda: random.uniform(82, 89),
            "systolic_bp": lambda: random.choice([random.uniform(185, 215), random.uniform(65, 78)]),
            "diastolic_bp": lambda: random.choice([random.uniform(95, 115), random.uniform(40, 55)]),
            "temperature": lambda: random.choice([random.uniform(38.6, 40.0), random.uniform(34.0, 34.9)]),
        }[vital]()


def delivery_callback(err, msg):
    """Kafka producer delivery report callback."""
    if err is not None:
        metrics.increment("vitals_producer", "send_errors")
        vitals_send_errors_total.inc()
        logger.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
        )
    else:
        metrics.increment("vitals_producer", "records_sent")
        vitals_records_sent_total.inc()


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
    last_condition = {p.patient_id: p.condition for p in patients}
    while not shutdown:
        cycle += 1
        for patient in patients:
            reading = patient.generate_reading()

            if reading["condition"] != last_condition[patient.patient_id]:
                logger.info(
                    "patient_condition_changed",
                    patient_id=patient.patient_id,
                    previous=last_condition[patient.patient_id],
                    condition=reading["condition"],
                )
                last_condition[patient.patient_id] = reading["condition"]

            producer.produce(
                topic=VITALS_TOPIC,
                key=reading["patient_id"].encode("utf-8"),
                value=json.dumps(reading).encode("utf-8"),
                callback=delivery_callback,
            )

            if reading["is_abnormal"]:
                vitals_abnormal_emitted_total.inc()
                logger.warning(
                    "abnormal_reading_emitted",
                    event_id=reading["event_id"],
                    patient_id=reading["patient_id"],
                    heart_rate=reading["heart_rate"],
                    spo2=reading["spo2"],
                    systolic_bp=reading["systolic_bp"],
                    condition=reading["condition"],
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
            heartbeat("vitals_producer")
            push_metrics("vitals_producer")

        time.sleep(EMIT_INTERVAL)

    # Graceful shutdown — flush remaining messages
    remaining = producer.flush(timeout=10)
    logger.info("vitals_producer_stopped", unflushed=remaining)


if __name__ == "__main__":
    from ingestion.kafka_config import setup_kafka
    setup_kafka()
    run_producer()
