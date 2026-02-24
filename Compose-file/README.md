# Kafka & MinIO Setup Guide

This directory contains the Docker Compose configuration for running Kafka and MinIO in the RUBAP project.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.7+ with `kafka-python` and `minio` libraries (optional, for direct connections)

## Quick Start

### 1. Start All Services

```bash
cd RUBAP/Compose-file
docker-compose -f kafka-compose.yml up -d
```

This will start:
- **Kafka Broker** on ports `9092` (external) and `9093` (controller)
- **kafka-init** — one-shot container that creates the `user-events` topic
- **MinIO** on ports `9000` (S3 API) and `9001` (web console)
- **minio-init** — one-shot container that creates the `user-events-lake` bucket

### 2. Verify Services are Running

```bash
docker-compose -f kafka-compose.yml ps
```

All services should show as running (or `Exited (0)` for the init containers).

### 3. Access MinIO Web Console

Open [http://localhost:9001](http://localhost:9001) in your browser.

| Field    | Value        |
|----------|--------------|
| Username | `minioadmin` |
| Password | `minioadmin` |

The `user-events-lake` bucket is created automatically on first start.

### 4. Start the Data Lake Consumer

In a separate terminal, run the Kafka → MinIO consumer (after installing dependencies):

```bash
pip install -r RUBAP/requirements.txt
python RUBAP/kafka_to_minio.py
```

The consumer reads from the `user-events` topic and writes batched JSON files to the `user-events-lake` MinIO bucket. See `kafka_to_minio.py` for available environment variables.

> **Alternative:** `kafka_to_minio.py` can be replaced with **Kafka Connect + S3 Sink Connector** for a fully managed, configuration-driven pipeline without custom code.

### 5. Create a Topic (Optional)

Kafka is configured to auto-create topics, but you can manually create one:

```bash
docker exec -it kafka-broker kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1
```

### 6. Connect Data Generator to Kafka

#### Option A: Direct Kafka Connection (Recommended)

```bash
# Install kafka-python if not already installed
pip install kafka-python

# Run data generator with Kafka connection
python RUBAP/data_generator.py \
  --user-id user_001 \
  --interval 2 \
  --kafka-bootstrap-servers localhost:9092 \
  --kafka-topic user-events
```

#### Option B: Using Kafka Console Producer

```bash
# Generate events and pipe to Kafka
python RUBAP/data_generator.py --user-id user_001 --interval 2 | \
  docker exec -i kafka-broker kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-events
```

### 7. Consume Events from Kafka

To verify events are being produced, consume from the topic:

```bash
docker exec -it kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --from-beginning
```

## Multiple Users

To simulate multiple users, run multiple instances:

```bash
# Terminal 1
python RUBAP/data_generator.py --user-id user_001 --interval 2 \
  --kafka-bootstrap-servers localhost:9092 --kafka-topic user-events

# Terminal 2
python RUBAP/data_generator.py --user-id user_002 --interval 2 \
  --kafka-bootstrap-servers localhost:9092 --kafka-topic user-events

# Terminal 3
python RUBAP/data_generator.py --user-id user_003 --interval 2 \
  --kafka-bootstrap-servers localhost:9092 --kafka-topic user-events
```

## Stop Services

```bash
docker-compose -f kafka-compose.yml down
```

To remove volumes (delete all data, including MinIO objects):
```bash
docker-compose -f kafka-compose.yml down -v
```

## Troubleshooting

### Check Kafka Logs
```bash
docker-compose -f kafka-compose.yml logs kafka
```

### Check MinIO Logs
```bash
docker-compose -f kafka-compose.yml logs minio
```

### List Topics
```bash
docker exec -it kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Describe Topic
```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic user-events
```

## Configuration

### Kafka
- **Mode**: KRaft (no Zookeeper)
- **Auto-create topics**: Enabled
- **Default partitions**: 3
- **Replication factor**: 1 (suitable for development)
- **Ports**: `9092` (external), `9093` (controller)

### MinIO
- **API port**: `9000`
- **Web console port**: `9001`
- **Default credentials**: `minioadmin` / `minioadmin`
- **Default bucket**: `user-events-lake`

To modify these settings, edit `kafka-compose.yml` and restart the services.



