# Event Streaming Data Pipeline

Kafka -> MongoDB

This is a small eduactional data engineering project that demostrates a streaming pipeline built around an event-driven architescture.

The pipeline reads data from a CSV file, publishes events to Kafka (Redpanda), and a consumer processes those events and stores them in MongoDB.

The entire system is fully containerized and runs with Docker Compose.

## Architecture

CSV -> Producer -> Kafka (RedPanda) -> Consumer -> MongoDB

- Producer:  Reads CSV data and publishes events to a Kafka topic.

- RedPanda: Kafka-compatible message broker.

- Consumer: Reads events from Kafka and writes them to MongoDB.

- MongoDB: Stores processed event data.

## Technologies used

- Python 3
- RedPadna (Kafka API)
- MongoDB
- confluent-kafka - Kafka producer/consumer client
- pymongo - MongoDB driver
- Docker & Docker Compose

## Running the Project

### Build and start all services 

```sh
docker compose up --build
```

This starts:

- RedPanda (Kafka broker)
- MongoDB
- Producer container
- Consumer container

### Producer. Event Format

```JSON
{
    "event_id": "uuid",
    "event_type": "insert",
    "produced_at": "ISO8601",
    "payload": {
        "..." : "CSV row data"
    }
}
```

### Redpanda Sanity-check

```sh
docker compose exec redpanda rpk topic list

docker compose exec redpanda rpk topic consume events --brokers redpanda:9092
```

### Reliability Features

The pipeline includes:

- Retry logic for Kafka connection
- Retry logic for MongoDB connection
- Resilience to service startup order