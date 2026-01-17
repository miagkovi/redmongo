"""
Kafka client setup for producer and consumer.
"""
from tenacity import retry, stop_after_attempt, wait_fixed
from kafka import KafkaProducer, KafkaConsumer


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_kafka_producer(broker) -> KafkaProducer:
    """Connects to Kafka and returns the producer."""
    print("Attempting to connect to Kafka Producer...")
    return KafkaProducer(bootstrap_servers=broker)

def get_kafka_consumer(topic, broker) -> KafkaConsumer:
    """Connects to Kafka and returns the consumer."""
    print("Attempting to connect to Kafka Consumer...")
    return KafkaConsumer(topic,
                         bootstrap_servers=broker)
