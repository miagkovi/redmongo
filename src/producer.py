"""
Read CSV data and produce events to Kafka.
"""


def run_producer():
    for row in read_csv():
        event = build_event(row)
        kafka_producer.send(topic, event)
