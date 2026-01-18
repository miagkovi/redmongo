"""
Read CSV data and produce events to Kafka.
"""
import csv
import time
import uuid
from pathlib import Path
from kafka_client import get_kafka_producer


def read_csv(file_path: Path):
    """Reads CSV file and yields each row as a dictionary."""
    if not file_path.exists() or file_path.suffix != '.csv':
        raise ValueError(f"CSV file not found: {file_path}")
    with open(file_path, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row


def build_event(row: dict):
    """Builds an event dictionary from a CSV row."""
    if row is None or not isinstance(row, dict):
        raise RuntimeError("Invalid row data provided for event building.")
    event = {
        "event_id": uuid.uuid4(),
        "event_type": "data_record",
        "produced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "payload": row
    }
    return event


def run_producer(file_path, kafka_topic, kafka_broker):
    """Reads CSV data and sends events to Kafka."""
    kafka_producer = get_kafka_producer(broker=kafka_broker)
    for row in read_csv(file_path):
        event = build_event(row)
        kafka_producer.send(kafka_topic, event)
        time.sleep(1)  # Simulate delay
