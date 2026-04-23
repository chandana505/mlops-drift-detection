"""
Single entry point for local development.
Runs Kafka producer and consumer concurrently using threads.
Use Ctrl+C to shut down gracefully.
"""

from dotenv import load_dotenv

# Load environment variables before importing modules that read os.getenv at import time
load_dotenv()

import threading
import signal
import time
import logging
import queue
from logging_config import setup_logging
from producer import run_producer
from consumer import run_consumer

setup_logging()
logger = logging.getLogger(__name__)

stop_event = threading.Event()
metrics_queue = queue.Queue()


def signal_handler(sig, frame):
    logger.info("[STOP] Shutdown signal received. Stopping threads...")
    stop_event.set()


# Register graceful shutdown handlers
signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    logger.info("[START] Starting MLOps Drift Detection Pipeline (Producer + Consumer)")

    producer_thread = threading.Thread(
        target=run_producer, args=(stop_event, metrics_queue), daemon=True, name="Producer"
    )
    consumer_thread = threading.Thread(
        target=lambda: run_consumer(stop_event=stop_event, metrics_queue=metrics_queue),
        daemon=True,
        name="Consumer"
    )

    producer_thread.start()
    consumer_thread.start()

    try:
        # Keep main thread alive until stop_event is set (e.g., by Ctrl+C)
        while not stop_event.is_set():
            stop_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()

    # Allow producer to finish first, then give consumer a grace period to drain
    producer_thread.join(timeout=10)
    if producer_thread.is_alive():
        logger.warning("Producer did not stop gracefully within timeout.")

    # Brief grace period for consumer to process in-flight messages
    time.sleep(2)
    stop_event.set()

    consumer_thread.join(timeout=10)
    if consumer_thread.is_alive():
        logger.warning("Consumer did not stop gracefully within timeout.")

    logger.info("[OK] Pipeline stopped.")

