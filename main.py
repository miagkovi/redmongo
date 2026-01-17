"""
Main entry point for the application.
Decides whether to run as a producer or consumer based on configuration.
"""
import os
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
ROLE = os.getenv("ROLE")

@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_mongodb():
    print("Attempting to connect to MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    return client

@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_kafka_producer():
    print("Attempting to connect to Kafka Producer...")
    return KafkaProducer(bootstrap_servers=KAFKA_BROKER)

def run_producer():
    kafka_producer = get_kafka_producer()
    topic = "events"
    for row in read_csv():
        event = build_event(row)
        kafka_producer.send(topic, event.encode('utf-8'))
        time.sleep(1)  # Simulate delay

def run_consumer():
    kafka_consumer = KafkaConsumer(
        'events',
        bootstrap_servers=KAFKA_BROKER
    )
    mongo_client = get_mongodb()
    db = mongo_client.mydatabase
    collection = db.events

    for msg in kafka_consumer:
        event = msg.value.decode('utf-8')
        if validate_event(event):
            collection.insert_one({"event": event})
        else:
            print(f"Invalid event: {event}")

if __name__ == "__main__":
    role = ROLE

    if role == "producer":
        run_producer()
    elif role == "consumer":
        run_consumer()
    else:
        raise RuntimeError(f"Unknown role: {role}")