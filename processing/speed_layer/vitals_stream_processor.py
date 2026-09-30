"""
Speed layer — Spark Structured Streaming processor.

Reads bedside vital-sign events from the Kafka topic `patient-vitals` and
runs three streaming queries off the same parsed stream:

  1. raw_archive  — appends every raw reading, untouched, to the Parquet
                    MASTER DATASET (data/master/vitals, partitioned by
                    event_date). This is the immutable, replayable source of
                    truth the Lambda batch layer recomputes from.
  2. vitals_agg   — 30 s tumbling-window avg/min/max per patient
                    (10 s watermark) -> vitals_aggregates (real-time view).
  3. vitals_alerts — per-reading threshold checks -> de-duplicated,
                    escalating alerts (one open alert per patient+vital).

Why UPSERT for the aggregates:
  Update output mode re-emits a window on every trigger while it is still
  open (partial counts first, final counts later). A plain INSERT of the
  second emission violates UNIQUE(patient_id, window_start), which used to
  crash the query on its second non-empty micro-batch. Each emission now
  overwrites the previous one (INSERT ... ON CONFLICT DO UPDATE), so the
  dashboard sees a window within ~10 s of it opening and the final numbers
  once it closes.

Timezones:
  The Spark session runs in UTC and every timestamp is formatted to a UTC
  string *inside Spark* before leaving the JVM, so neither the JVM default
  timezone nor Python's local time can shift stored times (Postgres columns
  are naive UTC).

Delivery guarantees:
  Kafka offsets are checkpointed per query. The DB sinks are idempotent
  (upsert) or append-only alerts; the Parquet archive is at-least-once on
  a replayed micro-batch, so the batch layer de-duplicates on event_id.
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from storage.db import execute_batch, get_connection  # noqa: E402
from observability.logging_config import get_logger  # noqa: E402
from observability.prometheus_metrics import (  # noqa: E402
    speed_aggregates_written_total,
    speed_alerts_written_total,
    speed_raw_records_archived_total,
    heartbeat,
    push as push_metrics,
)

logger = get_logger("processing.speed_layer")

# ── Configuration ──────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VITALS_TOPIC = os.getenv("KAFKA_VITALS_TOPIC", "patient-vitals")
STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")
CHECKPOINT_DIR = os.path.abspath(os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints"))
MASTER_DATASET_DIR = os.path.abspath(os.getenv("MASTER_DATASET_DIR", "data/master/vitals"))

WINDOW_DURATION = "30 seconds"
WATERMARK_DELAY = "10 seconds"

TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"  # rendered in the session timezone (UTC)

# ── Vitals event schema ───────────────────────────────────────────
VITALS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("patient_id", StringType(), False),
    StructField("heart_rate", DoubleType(), False),
    StructField("spo2", DoubleType(), False),
    StructField("systolic_bp", DoubleType(), False),
    StructField("diastolic_bp", DoubleType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("timestamp", StringType(), False),
    StructField("is_abnormal", BooleanType(), True),
])

# ── Clinical alert rules ──────────────────────────────────────────
# (vital, direction, alert threshold, critical threshold)
# A breach of the alert threshold is a WARNING; past the critical
# threshold it is CRITICAL. Values mirror processing/clinical_rules.py.
ALERT_RULES = [
    ("heart_rate",  "above", 120.0, 150.0),
    ("heart_rate",  "below",  50.0,  40.0),
    ("spo2",        "below",  90.0,  90.0),   # any SpO2 < 90 % is critical
    ("systolic_bp", "above", 180.0, 200.0),
    ("systolic_bp", "below",  80.0,  70.0),
    ("temperature", "above",  38.5,  39.5),
    ("temperature", "below",  35.0,  34.5),
]


def create_spark_session() -> SparkSession:
    """Build a local SparkSession with the Kafka connector, pinned to UTC."""
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "hospital-vitals-speed-layer"))
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "4")  # matches Kafka partitions; default 200 is wasteful here
        .getOrCreate()
    )


def build_vitals_stream(spark: SparkSession):
    """Parse the raw Kafka JSON into typed columns plus an event-time column."""
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", VITALS_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", VITALS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        # Cleaning: drop malformed / unparseable records instead of failing.
        .filter(F.col("patient_id").isNotNull() & F.col("event_time").isNotNull())
    )


# ── Sink 1: raw master dataset (Parquet) ──────────────────────────

def write_raw_archive(batch_df, batch_id):
    """Append the micro-batch's raw readings to the Parquet master dataset."""
    if batch_df.isEmpty():
        return
    batch_df = batch_df.withColumn("event_date", F.to_date("event_time"))
    batch_df.write.mode("append").partitionBy("event_date").parquet(MASTER_DATASET_DIR)
    rows = batch_df.count()
    speed_raw_records_archived_total.inc(rows)
    heartbeat("speed_layer")
    push_metrics("speed_layer")
    logger.info("raw_batch_archived", batch_id=batch_id, rows=rows, path=MASTER_DATASET_DIR)


# ── Sink 2: windowed aggregates (upsert) ──────────────────────────

UPSERT_AGGREGATES_SQL = """
    INSERT INTO vitals_aggregates
        (patient_id, window_start, window_end, avg_heart_rate, avg_spo2,
         avg_systolic_bp, avg_diastolic_bp, avg_temperature, min_spo2,
         max_heart_rate, reading_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (patient_id, window_start) DO UPDATE SET
        window_end       = EXCLUDED.window_end,
        avg_heart_rate   = EXCLUDED.avg_heart_rate,
        avg_spo2         = EXCLUDED.avg_spo2,
        avg_systolic_bp  = EXCLUDED.avg_systolic_bp,
        avg_diastolic_bp = EXCLUDED.avg_diastolic_bp,
        avg_temperature  = EXCLUDED.avg_temperature,
        min_spo2         = EXCLUDED.min_spo2,
        max_heart_rate   = EXCLUDED.max_heart_rate,
        reading_count    = EXCLUDED.reading_count,
        created_at       = (NOW() AT TIME ZONE 'UTC')
"""


def write_aggregates_to_db(batch_df, batch_id):
    """
    foreachBatch sink — upsert the updated windows into vitals_aggregates.

    The micro-batch is at most (patients x open windows) rows — a few dozen —
    so collecting it to the driver and writing with one psycopg2 batch is
    cheaper and simpler than a JDBC staging table.
    """
    rows = (
        batch_df
        .select(
            "patient_id",
            F.date_format("window.start", TS_FORMAT).alias("window_start"),
            F.date_format("window.end", TS_FORMAT).alias("window_end"),
            "avg_heart_rate", "avg_spo2", "avg_systolic_bp", "avg_diastolic_bp",
            "avg_temperature", "min_spo2", "max_heart_rate", "reading_count",
        )
        .collect()
    )
    if not rows:
        return

    execute_batch(UPSERT_AGGREGATES_SQL, [tuple(r) for r in rows])
    speed_aggregates_written_total.inc(len(rows))
    heartbeat("speed_layer")
    push_metrics("speed_layer")
    logger.info("aggregates_upserted", batch_id=batch_id, rows=len(rows))


# ── Sink 3: threshold alerts ──────────────────────────────────────

# Alarm management (why there is not one row per breaching reading):
#   - de-duplication: at most ONE open alert per (patient, vital). A repeat
#     breach within ALERT_COOLDOWN_MINUTES updates that alert (occurrences,
#     last_seen_at, latest value) instead of inserting a new row.
#   - escalation: a single breaching reading only raises a WARNING (it may be a
#     motion/probe artefact). The alert becomes CRITICAL once the breach is
#     confirmed: repeated at a critical level, or repeated ESCALATE_AFTER times.
ALERT_COOLDOWN_MINUTES = 5
ESCALATE_AFTER = 3

UPDATE_OPEN_ALERT_SQL = f"""
    UPDATE alerts SET
        occurrences  = occurrences + %(n)s,
        last_seen_at = %(ts)s,
        vital_value  = %(value)s,
        message      = %(message)s,
        event_id     = %(event_id)s,
        severity     = CASE
            WHEN severity = 'CRITICAL' THEN 'CRITICAL'
            WHEN %(critical_level)s AND occurrences + %(n)s >= 2 THEN 'CRITICAL'
            WHEN occurrences + %(n)s >= {ESCALATE_AFTER} THEN 'CRITICAL'
            ELSE 'WARNING' END
    WHERE id = (
        SELECT id FROM alerts
        WHERE patient_id = %(patient_id)s AND vital_name = %(vital)s
          AND alert_type = 'VITAL_THRESHOLD' AND acknowledged = FALSE
          AND COALESCE(last_seen_at, triggered_at)
              >= %(ts)s::timestamp - INTERVAL '{ALERT_COOLDOWN_MINUTES} minutes'
        ORDER BY triggered_at DESC LIMIT 1)
    RETURNING id
"""

INSERT_ALERT_SQL = """
    INSERT INTO alerts
        (patient_id, alert_type, severity, message, vital_name, vital_value,
         threshold_value, triggered_at, last_seen_at, occurrences, acknowledged, event_id)
    VALUES (%(patient_id)s, 'VITAL_THRESHOLD', %(severity)s, %(message)s, %(vital)s,
            %(value)s, %(threshold)s, %(ts)s, %(ts)s, %(n)s, FALSE, %(event_id)s)
"""


def build_alerts(vitals):
    """
    One alert row per (reading, breached rule). A reading that breaches two
    rules (e.g. high HR and low SpO2) yields two alerts rather than hiding
    the second behind the first.
    """
    candidates = []
    for vital, direction, threshold, critical in ALERT_RULES:
        value = F.col(vital)
        breached = value > threshold if direction == "above" else value < threshold
        is_critical = value > critical if direction == "above" else value < critical
        if vital == "spo2":
            is_critical = breached
        candidates.append(
            F.when(
                breached,
                F.struct(
                    F.lit(vital).alias("vital_name"),
                    value.alias("vital_value"),
                    F.lit(threshold).alias("threshold_value"),
                    F.when(is_critical, "CRITICAL").otherwise("WARNING").alias("severity"),
                    F.concat(
                        F.lit(f"{vital} is {direction} threshold: value="),
                        F.format_number(value, 1),
                        F.lit(f", threshold={threshold:.1f}"),
                    ).alias("message"),
                ),
            )
        )

    return (
        vitals
        .withColumn("breaches", F.filter(F.array(*candidates), lambda b: b.isNotNull()))
        .filter(F.size("breaches") > 0)
        .withColumn("breach", F.explode("breaches"))
        .select(
            "patient_id",
            "breach.severity",
            "breach.message",
            "breach.vital_name",
            "breach.vital_value",
            "breach.threshold_value",
            # triggered_at = when the reading was taken, not when Spark saw it
            F.date_format("event_time", TS_FORMAT).alias("triggered_at"),
            "event_id",
        )
    )


def write_alerts_to_db(batch_df, batch_id):
    """
    foreachBatch sink: de-duplicate and escalate threshold breaches
    (see the alarm-management note above), then update or open alerts.
    """
    rows = batch_df.collect()
    if not rows:
        return

    # Collapse the micro-batch to one breach per (patient, vital); keep the
    # most severe reading and count how many readings breached.
    grouped: dict[tuple, dict] = {}
    for r in rows:
        key = (r["patient_id"], r["vital_name"])
        g = grouped.setdefault(key, {"n": 0, "row": r, "critical_level": False})
        g["n"] += 1
        if r["severity"] == "CRITICAL":
            g["critical_level"] = True
            g["row"] = r

    opened = updated = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for (patient_id, vital), g in grouped.items():
                r = g["row"]
                params = {
                    "patient_id": patient_id, "vital": vital, "n": g["n"],
                    "ts": r["triggered_at"], "value": r["vital_value"],
                    "threshold": r["threshold_value"], "message": r["message"],
                    "event_id": r["event_id"], "critical_level": g["critical_level"],
                    # A brand-new alert is only CRITICAL if already confirmed
                    # within this micro-batch.
                    "severity": "CRITICAL" if (g["critical_level"] and g["n"] >= 2)
                                or g["n"] >= ESCALATE_AFTER else "WARNING",
                }
                cur.execute(UPDATE_OPEN_ALERT_SQL, params)
                if cur.fetchone():
                    updated += 1
                else:
                    cur.execute(INSERT_ALERT_SQL, params)
                    opened += 1
                    logger.warning(
                        "vital_alert_opened", batch_id=batch_id, event_id=r["event_id"],
                        patient_id=patient_id, vital=vital, value=r["vital_value"],
                        severity=params["severity"],
                    )

    speed_alerts_written_total.inc(opened)
    heartbeat("speed_layer")
    push_metrics("speed_layer")
    logger.info("alerts_processed", batch_id=batch_id, breaches=len(rows),
                alerts_opened=opened, alerts_updated=updated)


# ── Entry point ───────────────────────────────────────────────────

def run_stream_processor():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    vitals = build_vitals_stream(spark)

    raw_query = (
        vitals.writeStream
        .queryName("raw_archive")
        .foreachBatch(write_raw_archive)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_archive")
        .trigger(processingTime="30 seconds")
        .start()
    )

    aggregated = (
        vitals
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy("patient_id", F.window("event_time", WINDOW_DURATION))
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.avg("spo2").alias("avg_spo2"),
            F.avg("systolic_bp").alias("avg_systolic_bp"),
            F.avg("diastolic_bp").alias("avg_diastolic_bp"),
            F.avg("temperature").alias("avg_temperature"),
            F.min("spo2").alias("min_spo2"),
            F.max("heart_rate").alias("max_heart_rate"),
            F.count("*").alias("reading_count"),
        )
    )
    agg_query = (
        aggregated.writeStream
        .queryName("vitals_agg")
        .outputMode("update")
        .foreachBatch(write_aggregates_to_db)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/vitals_agg")
        .trigger(processingTime="10 seconds")
        .start()
    )

    alert_query = (
        build_alerts(vitals).writeStream
        .queryName("vitals_alerts")
        .outputMode("append")
        .foreachBatch(write_alerts_to_db)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/vitals_alerts")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info(
        "stream_processor_started",
        topic=VITALS_TOPIC,
        master_dataset=MASTER_DATASET_DIR,
        window=WINDOW_DURATION,
        watermark=WATERMARK_DELAY,
    )
    print("Stream processor started (raw archive 30s, aggregates 10s, alerts 5s). Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping stream processor...")
    finally:
        for q in (raw_query, agg_query, alert_query):
            if q.isActive:
                q.stop()
            if q.exception():
                logger.error("streaming_query_failed", query=q.name, error=str(q.exception()))
        spark.stop()
        logger.info("stream_processor_stopped")


if __name__ == "__main__":
    run_stream_processor()
