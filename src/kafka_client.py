"""
Kafka client setup for producer and consumer.
"""
from tenacity import retry, stop_after_attempt, wait_fixed
from confluent_kafka import Producer, Consumer


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_kafka_producer(broker) -> Producer:
    """Connects to Kafka and returns the producer."""
    print("Attempting to connect to Kafka Producer...")
    return Producer(
        {"bootstrap.servers": broker}
    )

def get_kafka_consumer(topic, broker, group_id) -> Consumer:
    """Connects to Kafka and returns the consumer."""
    print("Attempting to connect to Kafka Consumer...")
    return Consumer({
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })