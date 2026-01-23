"""
Main entry point for the application.
Decides whether to run as a producer or consumer based on configuration.
"""
from src.producer import run_producer
from src.consumer import run_consumer
from config import ROLE, KAFKA_TOPIC, MONGO_URI, KAFKA_BROKER, GROUP_ID


if __name__ == "__main__":
    role = ROLE.lower()

    if role == "producer":
        run_producer(file_path="./data/netflix_tv_shows.csv",
                     kafka_topic=KAFKA_TOPIC,
                     kafka_broker=KAFKA_BROKER)
    elif role == "consumer":
        run_consumer(kafka_topic=KAFKA_TOPIC,
                     kafka_broker=KAFKA_BROKER,
                     group_id=GROUP_ID,
                     mongo_uri=MONGO_URI)
    else:
        raise RuntimeError(f"Unknown role: {role}")
