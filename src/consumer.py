"""
Reads events from Kafka, validates them, and saves them to MongoDB.
"""


def run_consumer():
    for msg in kafka_consumer:
        validate_event(msg)
        save_to_mongo(msg)