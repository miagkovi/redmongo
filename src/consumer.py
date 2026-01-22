"""
Reads events from Kafka, validates them, and saves them to MongoDB.
"""
from src.kafka_client import get_kafka_consumer
from src.mongo_engine import get_mongodb
from src.schemas import REQUIRED_EVENT_FIELDS


def is_event_valid(event: dict) -> bool:
    """Validates the event against the required schema."""
    if not isinstance(event, dict):
        return False
    return all(field in event for field in REQUIRED_EVENT_FIELDS)


def run_consumer(kafka_topic, kafka_broker, mongo_uri):
    """Consumes events from Kafka, validates, and stores them in MongoDB."""
    kafka_consumer = get_kafka_consumer(topic=kafka_topic,
                                        broker=kafka_broker)
    mongo_client = get_mongodb(uri=mongo_uri)
    db = mongo_client.mydatabase
    collection = db.events

    for msg in kafka_consumer:
        event = msg.value.decode('utf-8')
        if is_event_valid(event):
            collection.insert_one({"event": event})
        else:
            print(f"Invalid event: {event}")
