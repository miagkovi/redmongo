"""
Reads events from Kafka, validates them, and saves them to MongoDB.
"""
from kafka_client import get_kafka_consumer
from mongo_engine import get_mongodb


def run_consumer(kafka_topic, kafka_broker, mongo_uri):
    """Consumes events from Kafka, validates, and stores them in MongoDB."""
    kafka_consumer = get_kafka_consumer(topic=kafka_topic,
                                        broker=kafka_broker)
    mongo_client = get_mongodb(uri=mongo_uri)
    db = mongo_client.mydatabase
    collection = db.events

    for msg in kafka_consumer:
        event = msg.value.decode('utf-8')
        if validate_event(event):
            collection.insert_one({"event": event})
        else:
            print(f"Invalid event: {event}")
