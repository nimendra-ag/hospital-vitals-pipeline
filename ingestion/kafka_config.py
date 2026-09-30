"""
Kafka topic creation and configuration.

Creates the required topics with appropriate partition counts
before any producer or consumer starts. Partitioning strategy:
  - patient-vitals:  partitioned by patient_id so that all readings
                     for a single patient land on the same partition,
                     enabling per-patient stateful stream processing.

The daily lab feed deliberately does NOT go through Kafka: it is a
once-a-day file drop consumed by the Airflow batch layer (see README,
"Architecture decision"), so no lab topic is created.
"""

import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from observability.logging_config import get_logger

load_dotenv()
logger = get_logger("ingestion.kafka_config")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def wait_for_kafka(timeout: int = 60):
    """Block until the Kafka broker is reachable or timeout expires."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                logger.info(
                    "kafka_connected",
                    brokers=len(metadata.brokers),
                    bootstrap=BOOTSTRAP_SERVERS,
                )
                return
        except Exception:
            pass
        logger.info("waiting_for_kafka", remaining=int(deadline - time.time()))
        time.sleep(3)

    raise RuntimeError(f"Kafka not reachable at {BOOTSTRAP_SERVERS} after {timeout}s")


def create_topics():
    """Create pipeline topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics.keys())

    vitals_topic = os.getenv("KAFKA_VITALS_TOPIC", "patient-vitals")
    num_partitions = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))

    topics_to_create = []

    if vitals_topic not in existing:
        topics_to_create.append(
            NewTopic(
                topic=vitals_topic,
                num_partitions=num_partitions,
                replication_factor=1,
                config={
                    "retention.ms": str(24 * 60 * 60 * 1000),  # 24 hours
                    "cleanup.policy": "delete",
                },
            )
        )

    if not topics_to_create:
        logger.info("topics_already_exist", topics=[vitals_topic])
        return

    futures = admin.create_topics(topics_to_create)

    for topic_name, future in futures.items():
        try:
            future.result()  # block until topic is created
            logger.info("topic_created", topic=topic_name)
        except Exception as e:
            logger.error("topic_creation_failed", topic=topic_name, error=str(e))
            raise


def setup_kafka():
    """Wait for Kafka, then create required topics."""
    wait_for_kafka()
    create_topics()


if __name__ == "__main__":
    setup_kafka()
    print("Kafka topics ready.")
