"""
Reads events from Kafka, validates them, and saves them to MongoDB.
"""
import json
from src.kafka_client import get_kafka_consumer
from src.mongo_engine import get_mongodb
from src.schemas import REQUIRED_EVENT_FIELDS


def is_event_valid(event: dict) -> bool:
    """Validates the event against the required schema."""
    if not isinstance(event, dict):
        return False
    return all(field in event for field in REQUIRED_EVENT_FIELDS)


def run_consumer(kafka_topic, kafka_broker, group_id, mongo_uri):
    """Consumes events from Kafka, validates, and stores them in MongoDB."""
    consumer = get_kafka_consumer(topic=kafka_topic,
                                  broker=kafka_broker,
                                  group_id=group_id)
    consumer.subscribe([kafka_topic])
    mongo_client = get_mongodb(uri=mongo_uri)
    db = mongo_client.mydatabase
    collection = db.events

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            event = json.loads(msg.value().decode('utf-8'))
            if is_event_valid(event):
                collection.insert_one({"event": event})
                print(f"Stored event: {event['event_id']}")
            else:
                print(f"Invalid event: {event}")
    finally:
        consumer.close()
        mongo_client.close()
