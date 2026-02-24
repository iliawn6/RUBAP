"""
Kafka → MinIO Data Lake Consumer

Reads events from the Kafka 'user-events' topic, batches them, and uploads
each batch as a JSON file to the MinIO 'user-events-lake' bucket.

Files are stored with Hive-style time partitioning:
  user-events/year=YYYY/month=MM/day=DD/<timestamp>-p<partition>-o<offset>.json

The consumer flushes a batch when either:
  - BATCH_SIZE messages have accumulated (default: 100), or
  - FLUSH_INTERVAL_SECONDS seconds have elapsed since the last flush (default: 30)

> NOTE: This consumer can be replaced by Kafka Connect + S3 Sink Connector.
> The S3 Sink Connector provides the same pipeline (Kafka topic → MinIO bucket)
> as a managed, configuration-driven service without custom code, at the cost of
> running an additional JVM container and managing connector configs via REST API.

Environment Variables:
  KAFKA_BOOTSTRAP        Kafka bootstrap server address (default: localhost:9092)
  KAFKA_TOPIC            Kafka topic to consume (default: user-events)
  KAFKA_GROUP_ID         Consumer group ID (default: minio-lake-consumer)
  MINIO_ENDPOINT         MinIO endpoint host:port (default: localhost:9000)
  MINIO_ACCESS_KEY       MinIO access key (default: minioadmin)
  MINIO_SECRET_KEY       MinIO secret key (default: minioadmin)
  MINIO_BUCKET           MinIO destination bucket (default: user-events-lake)
  BATCH_SIZE             Number of messages per batch file (default: 100)
  FLUSH_INTERVAL_SECONDS Max seconds between flushes (default: 30)
"""

import io
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "minio-lake-consumer")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "user-events-lake")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, flushing remaining messages…")
    _shutdown = True


def _build_object_name(batch: list[dict]) -> str:
    """Build a partitioned object path from the first message's timestamp."""
    now = datetime.now(tz=timezone.utc)
    first = batch[0]
    partition = first.get("_partition", 0)
    offset = first.get("_offset", 0)
    ts = now.strftime("%H%M%S")
    date_prefix = now.strftime("year=%Y/month=%m/day=%d")
    return f"user-events/{date_prefix}/{ts}-p{partition}-o{offset}.json"


def _upload_batch(client: Minio, batch: list[dict]) -> None:
    """Serialize and upload a batch of events to MinIO."""
    payload = json.dumps(batch, ensure_ascii=False, indent=2).encode("utf-8")
    object_name = _build_object_name(batch)
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    log.info("Uploaded %d events → %s/%s", len(batch), MINIO_BUCKET, object_name)


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("Connecting to Kafka at %s, topic '%s'", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    log.info(
        "Connecting to MinIO at %s, bucket '%s'", MINIO_ENDPOINT, MINIO_BUCKET
    )
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        log.info("Created bucket '%s'", MINIO_BUCKET)

    batch: list[dict] = []
    last_flush = time.monotonic()
    total_uploaded = 0

    log.info(
        "Consumer started. BATCH_SIZE=%d, FLUSH_INTERVAL=%ds",
        BATCH_SIZE,
        FLUSH_INTERVAL_SECONDS,
    )

    try:
        while not _shutdown:
            for message in consumer:
                event = message.value
                event["_partition"] = message.partition
                event["_offset"] = message.offset
                batch.append(event)

                elapsed = time.monotonic() - last_flush
                if len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SECONDS:
                    _upload_batch(minio_client, batch)
                    consumer.commit()
                    total_uploaded += len(batch)
                    batch = []
                    last_flush = time.monotonic()

                if _shutdown:
                    break

            # Flush on timeout tick if there is anything buffered
            if batch and (time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS):
                _upload_batch(minio_client, batch)
                consumer.commit()
                total_uploaded += len(batch)
                batch = []
                last_flush = time.monotonic()

    except S3Error as exc:
        log.error("MinIO error: %s", exc)
        sys.exit(1)
    finally:
        if batch:
            log.info("Flushing final batch of %d messages…", len(batch))
            try:
                _upload_batch(minio_client, batch)
                consumer.commit()
                total_uploaded += len(batch)
            except Exception as exc:  # noqa: BLE001
                log.error("Failed to flush final batch: %s", exc)
        consumer.close()
        log.info("Consumer shut down. Total events uploaded: %d", total_uploaded)


if __name__ == "__main__":
    main()
