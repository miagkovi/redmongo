"""
Main entry point for the application.
Decides whether to run as a producer or consumer based on configuration.
"""
from config import ROLE
from consumer import run_consumer
from producer import run_producer


if __name__ == "__main__":
    role = ROLE

    if role == "producer":
        run_producer()
    elif role == "consumer":
        run_consumer()
    else:
        raise RuntimeError(f"Unknown role: {role}")