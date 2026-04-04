"""
Kafka → MongoDB Hot Storage Consumer

Reads events from the Kafka 'user-events' topic, batches them, and inserts
into MongoDB as hot storage. Documents expire automatically after 1 hour via
a TTL index on '_ingested_at' (created by mongo-init).

The consumer flushes a batch when either:
  - BATCH_SIZE messages have accumulated (default: 100), or
  - FLUSH_INTERVAL_SECONDS seconds have elapsed since the last flush (default: 30)

Environment Variables:
  KAFKA_BOOTSTRAP        Kafka bootstrap server address (default: localhost:9092)
  KAFKA_TOPIC            Kafka topic to consume (default: user-events)
  KAFKA_GROUP_ID         Consumer group ID (default: mongo-hot-consumer)
  MONGO_URI              MongoDB connection URI (default: mongodb://localhost:27017)
  MONGO_DB               MongoDB database name (default: hot_storage)
  MONGO_COLLECTION       MongoDB collection name (default: user_events)
  BATCH_SIZE             Number of messages per batch insert (default: 100)
  FLUSH_INTERVAL_SECONDS Max seconds between flushes (default: 30)
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "mongo-hot-consumer")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "hot_storage")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "user_events")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, flushing remaining messages…")
    _shutdown = True


def _prepare_doc(event: dict, partition: int, offset: int) -> dict:
    """Enrich event with ingestion metadata for TTL and tracing."""
    doc = dict(event)
    doc["_partition"] = partition
    doc["_offset"] = offset
    doc["_ingested_at"] = datetime.now(tz=timezone.utc)
    return doc


def _insert_batch(collection, batch: list[dict]) -> None:
    """Insert a batch of documents into MongoDB."""
    collection.insert_many(batch)
    log.info("Inserted %d events → %s.%s", len(batch), MONGO_DB, MONGO_COLLECTION)


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

    log.info("Connecting to MongoDB at %s, db '%s', collection '%s'", MONGO_URI, MONGO_DB, MONGO_COLLECTION)
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    batch: list[dict] = []
    last_flush = time.monotonic()
    total_inserted = 0

    log.info(
        "Consumer started. BATCH_SIZE=%d, FLUSH_INTERVAL=%ds",
        BATCH_SIZE,
        FLUSH_INTERVAL_SECONDS,
    )

    try:
        while not _shutdown:
            for message in consumer:
                doc = _prepare_doc(message.value, message.partition, message.offset)
                batch.append(doc)

                elapsed = time.monotonic() - last_flush
                if len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SECONDS:
                    _insert_batch(collection, batch)
                    consumer.commit()
                    total_inserted += len(batch)
                    batch = []
                    last_flush = time.monotonic()

                if _shutdown:
                    break

            # Flush on timeout tick if there is anything buffered
            if batch and (time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS):
                _insert_batch(collection, batch)
                consumer.commit()
                total_inserted += len(batch)
                batch = []
                last_flush = time.monotonic()

    except PyMongoError as exc:
        log.error("MongoDB error: %s", exc)
        sys.exit(1)
    finally:
        if batch:
            log.info("Flushing final batch of %d messages…", len(batch))
            try:
                _insert_batch(collection, batch)
                consumer.commit()
                total_inserted += len(batch)
            except Exception as exc:  # noqa: BLE001
                log.error("Failed to flush final batch: %s", exc)
        consumer.close()
        mongo_client.close()
        log.info("Consumer shut down. Total events inserted: %d", total_inserted)


if __name__ == "__main__":
    main()
