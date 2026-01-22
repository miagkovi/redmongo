import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
ROLE = os.getenv("ROLE")