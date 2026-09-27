"""
Speed layer — Spark Structured Streaming processor.

Reads patient vital signs from the Kafka topic `patient-vitals`,
applies tumbling-window aggregation (30-second windows) per patient,
and performs threshold-based anomaly detection.

Outputs:
  - vitals_aggregates table:  windowed avg/min/max per patient
  - alerts table:             threshold breaches (critical vitals)

Processing guarantees:
  - Watermark of 10 seconds handles late-arriving data.
  - Checkpoint directory ensures exactly-once semantics on restart.

Clinical thresholds for alerting:
  Heart rate    > 120 or < 50     → WARNING / CRITICAL
  SpO2          < 90              → CRITICAL
  Systolic BP   > 180 or < 80    → WARNING / CRITICAL
  Temperature   > 38.5 or < 35.0 → WARNING
"""

import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    min as spark_min,
    max as spark_max,
    count,
    when,
    lit,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    BooleanType,
    TimestampType,
)

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VITALS_TOPIC = os.getenv("KAFKA_VITALS_TOPIC", "patient-vitals")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints")
DATABASE_URL = os.getenv("DATABASE_URL")

# JDBC connection properties for PostgreSQL (Neon)
JDBC_URL = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://", 1)
DB_PROPERTIES = {
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require",
}

# ── Vitals event schema ───────────────────────────────────────────
VITALS_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("heart_rate", FloatType(), False),
    StructField("spo2", FloatType(), False),
    StructField("systolic_bp", FloatType(), False),
    StructField("diastolic_bp", FloatType(), False),
    StructField("temperature", FloatType(), False),
    StructField("timestamp", StringType(), False),
    StructField("is_abnormal", BooleanType(), True),
])

# ── Clinical alert thresholds ─────────────────────────────────────
THRESHOLDS = {
    "heart_rate_high":   120.0,
    "heart_rate_low":    50.0,
    "spo2_low":          90.0,
    "systolic_bp_high":  180.0,
    "systolic_bp_low":   80.0,
    "temperature_high":  38.5,
    "temperature_low":   35.0,
}


def create_spark_session() -> SparkSession:
    """Build the SparkSession with Kafka + PostgreSQL JDBC packages."""
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "hospital-vitals-pipeline"))
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_vitals_stream(spark: SparkSession):
    """
    Read the raw vitals JSON stream from Kafka and parse it into
    a structured DataFrame with an event-time timestamp column.
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", VITALS_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), VITALS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
    )

    return parsed


def write_aggregates_to_db(batch_df, batch_id):
    """
    foreachBatch sink — writes windowed aggregates to PostgreSQL.

    Called by Spark for each micro-batch of the aggregation query.
    """
    if batch_df.isEmpty():
        return

    (
        batch_df
        .select(
            col("patient_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_heart_rate"),
            col("avg_spo2"),
            col("avg_systolic_bp"),
            col("avg_diastolic_bp"),
            col("avg_temperature"),
            col("min_spo2"),
            col("max_heart_rate"),
            col("reading_count"),
        )
        .write
        .jdbc(
            url=JDBC_URL,
            table="vitals_aggregates",
            mode="append",
            properties=DB_PROPERTIES,
        )
    )
    print(f"[Aggregates] Batch {batch_id}: wrote {batch_df.count()} rows")


def write_alerts_to_db(batch_df, batch_id):
    """
    foreachBatch sink — writes threshold-breach alerts to PostgreSQL.
    """
    if batch_df.isEmpty():
        return

    (
        batch_df
        .write
        .jdbc(
            url=JDBC_URL,
            table="alerts",
            mode="append",
            properties=DB_PROPERTIES,
        )
    )
    print(f"[Alerts] Batch {batch_id}: wrote {batch_df.count()} alerts")


def run_stream_processor():
    """
    Main entry point — starts two streaming queries:
      1. Windowed aggregation (30 s tumbling window, 10 s watermark)
      2. Per-reading threshold alerting
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    vitals = build_vitals_stream(spark)

    # ── Query 1: Windowed aggregation ──────────────────────────────
    aggregated = (
        vitals
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            col("patient_id"),
            window(col("event_time"), "30 seconds"),
        )
        .agg(
            avg("heart_rate").alias("avg_heart_rate"),
            avg("spo2").alias("avg_spo2"),
            avg("systolic_bp").alias("avg_systolic_bp"),
            avg("diastolic_bp").alias("avg_diastolic_bp"),
            avg("temperature").alias("avg_temperature"),
            spark_min("spo2").alias("min_spo2"),
            spark_max("heart_rate").alias("max_heart_rate"),
            count("*").alias("reading_count"),
        )
    )

    agg_query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_aggregates_to_db)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/vitals_agg")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Query 2: Threshold-based alerting ──────────────────────────
    alerts = (
        vitals
        .filter(
            (col("heart_rate") > THRESHOLDS["heart_rate_high"])
            | (col("heart_rate") < THRESHOLDS["heart_rate_low"])
            | (col("spo2") < THRESHOLDS["spo2_low"])
            | (col("systolic_bp") > THRESHOLDS["systolic_bp_high"])
            | (col("systolic_bp") < THRESHOLDS["systolic_bp_low"])
            | (col("temperature") > THRESHOLDS["temperature_high"])
            | (col("temperature") < THRESHOLDS["temperature_low"])
        )
        .select(
            col("patient_id"),
            # Determine which vital triggered and its severity
            when(
                (col("heart_rate") > THRESHOLDS["heart_rate_high"])
                | (col("heart_rate") < THRESHOLDS["heart_rate_low"]),
                lit("VITAL_THRESHOLD"),
            )
            .when(col("spo2") < THRESHOLDS["spo2_low"], lit("VITAL_THRESHOLD"))
            .when(
                (col("systolic_bp") > THRESHOLDS["systolic_bp_high"])
                | (col("systolic_bp") < THRESHOLDS["systolic_bp_low"]),
                lit("VITAL_THRESHOLD"),
            )
            .when(
                (col("temperature") > THRESHOLDS["temperature_high"])
                | (col("temperature") < THRESHOLDS["temperature_low"]),
                lit("VITAL_THRESHOLD"),
            )
            .alias("alert_type"),
            # Severity: SpO2 < 90 or HR extremes are CRITICAL, rest WARNING
            when(
                (col("spo2") < THRESHOLDS["spo2_low"])
                | (col("heart_rate") > 150)
                | (col("heart_rate") < 40),
                lit("CRITICAL"),
            )
            .otherwise(lit("WARNING"))
            .alias("severity"),
            # Build a human-readable message
            when(
                col("heart_rate") > THRESHOLDS["heart_rate_high"],
                concat_alert_msg("heart_rate", col("heart_rate"), lit(THRESHOLDS["heart_rate_high"]), lit("above")),
            )
            .when(
                col("heart_rate") < THRESHOLDS["heart_rate_low"],
                concat_alert_msg("heart_rate", col("heart_rate"), lit(THRESHOLDS["heart_rate_low"]), lit("below")),
            )
            .when(
                col("spo2") < THRESHOLDS["spo2_low"],
                concat_alert_msg("spo2", col("spo2"), lit(THRESHOLDS["spo2_low"]), lit("below")),
            )
            .when(
                col("systolic_bp") > THRESHOLDS["systolic_bp_high"],
                concat_alert_msg("systolic_bp", col("systolic_bp"), lit(THRESHOLDS["systolic_bp_high"]), lit("above")),
            )
            .when(
                col("systolic_bp") < THRESHOLDS["systolic_bp_low"],
                concat_alert_msg("systolic_bp", col("systolic_bp"), lit(THRESHOLDS["systolic_bp_low"]), lit("below")),
            )
            .when(
                col("temperature") > THRESHOLDS["temperature_high"],
                concat_alert_msg("temperature", col("temperature"), lit(THRESHOLDS["temperature_high"]), lit("above")),
            )
            .when(
                col("temperature") < THRESHOLDS["temperature_low"],
                concat_alert_msg("temperature", col("temperature"), lit(THRESHOLDS["temperature_low"]), lit("below")),
            )
            .otherwise(lit("Vital sign threshold breach detected"))
            .alias("message"),
            # Record which vital and its value
            when(
                (col("heart_rate") > THRESHOLDS["heart_rate_high"])
                | (col("heart_rate") < THRESHOLDS["heart_rate_low"]),
                lit("heart_rate"),
            )
            .when(col("spo2") < THRESHOLDS["spo2_low"], lit("spo2"))
            .when(
                (col("systolic_bp") > THRESHOLDS["systolic_bp_high"])
                | (col("systolic_bp") < THRESHOLDS["systolic_bp_low"]),
                lit("systolic_bp"),
            )
            .when(
                (col("temperature") > THRESHOLDS["temperature_high"])
                | (col("temperature") < THRESHOLDS["temperature_low"]),
                lit("temperature"),
            )
            .alias("vital_name"),
            when(
                (col("heart_rate") > THRESHOLDS["heart_rate_high"])
                | (col("heart_rate") < THRESHOLDS["heart_rate_low"]),
                col("heart_rate"),
            )
            .when(col("spo2") < THRESHOLDS["spo2_low"], col("spo2"))
            .when(
                (col("systolic_bp") > THRESHOLDS["systolic_bp_high"])
                | (col("systolic_bp") < THRESHOLDS["systolic_bp_low"]),
                col("systolic_bp"),
            )
            .when(
                (col("temperature") > THRESHOLDS["temperature_high"])
                | (col("temperature") < THRESHOLDS["temperature_low"]),
                col("temperature"),
            )
            .alias("vital_value"),
            current_timestamp().alias("triggered_at"),
            lit(False).alias("acknowledged"),
        )
    )

    alert_query = (
        alerts.writeStream
        .outputMode("append")
        .foreachBatch(write_alerts_to_db)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/vitals_alerts")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Stream processor started. Waiting for data...")
    print("  - Aggregation query: 30s windows, written every 10s")
    print("  - Alert query: threshold checks, written every 5s")
    print("Press Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping stream processor...")
        agg_query.stop()
        alert_query.stop()
        spark.stop()
        print("Stream processor stopped.")


def concat_alert_msg(vital_name: str, value_col, threshold_col, direction_col):
    """Build a formatted alert message string using Spark concat."""
    from pyspark.sql.functions import concat, lit, format_number

    return concat(
        lit(f"{vital_name} is "),
        direction_col,
        lit(" threshold: value="),
        format_number(value_col, 1),
        lit(", threshold="),
        format_number(threshold_col, 1),
    )


if __name__ == "__main__":
    run_stream_processor()
