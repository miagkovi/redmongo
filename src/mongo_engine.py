from tenacity import retry, stop_after_attempt, wait_fixed
from pymongo import MongoClient


@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_mongodb(uri) -> MongoClient:
    """Connects to MongoDB and returns the client."""
    print("Attempting to connect to MongoDB...")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    return client
