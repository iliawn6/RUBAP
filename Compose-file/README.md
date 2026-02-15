# Kafka Setup Guide

This directory contains the Docker Compose configuration for running Kafka in the RUBAP project.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.7+ with `kafka-python` library (optional, for direct Kafka connection)

## Quick Start

### 1. Start Kafka Services

```bash
cd RUBAP/Compose-file
docker-compose -f Kafka-compose.yml up -d
```

This will start:
- **Zookeeper** on port `2181`
- **Kafka Broker** on port `9092`

### 2. Verify Kafka is Running

Check if services are healthy:
```bash
docker-compose -f Kafka-compose.yml ps
```

You should see both `zookeeper` and `kafka-broker` services running.

### 3. Create a Topic (Optional)

Kafka is configured to auto-create topics, but you can manually create one:

```bash
docker exec -it kafka-broker kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1
```

### 4. Connect Data Generator to Kafka

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

### 5. Consume Events from Kafka

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
docker-compose -f Kafka-compose.yml down
```

To remove volumes (delete all data):
```bash
docker-compose -f Kafka-compose.yml down -v
```

## Troubleshooting

### Check Kafka Logs
```bash
docker-compose -f Kafka-compose.yml logs kafka
```

### Check Zookeeper Logs
```bash
docker-compose -f Kafka-compose.yml logs zookeeper
```

### List Topics
```bash
docker exec -it kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Describe Topic
```bash
docker exec -it kafka-broker kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic user-events
```

## Configuration

The Kafka setup is configured with:
- **Auto-create topics**: Enabled
- **Default partitions**: 3
- **Replication factor**: 1 (suitable for development)
- **Port**: 9092 (external), 9093 (internal)

To modify these settings, edit `Kafka-compose.yml` and restart the services.



