"""
Read CSV data and produce events to Kafka.
"""
import time
from kafka_client import get_kafka_producer


def run_producer(kafka_topic, kafka_broker):
    """Reads CSV data and sends events to Kafka."""
    kafka_producer = get_kafka_producer(broker=kafka_broker)
    for row in read_csv():
        event = build_event(row)
        kafka_producer.send(kafka_topic, event.encode('utf-8'))
        time.sleep(1)  # Simulate delay
