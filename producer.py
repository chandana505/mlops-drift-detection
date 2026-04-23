import logging
import os
import pandas as pd
import json
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

# Load environment variables before reading producer config
load_dotenv()

# Configurable via environment variables for easy experimentation
MAX_RECORDS = int(os.getenv("PRODUCER_MAX_RECORDS", "2000"))
DRIFT_START = int(os.getenv("PRODUCER_DRIFT_START", "1000"))
DRIFT_SCALE = float(os.getenv("PRODUCER_DRIFT_SCALE", "5.0"))
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP", "0.005"))


def run_producer(stop_event=None, metrics_queue=None):
    """Stream records to Kafka with optional synthetic drift injection."""
    # Load dataset
    df = pd.read_csv("creditcard.csv").head(MAX_RECORDS)
    records = df.to_dict("records")

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic = "transactions"

    try:
        for i, data in enumerate(records):
            if stop_event is not None and stop_event.is_set():
                logger.info("Producer received stop signal. Exiting...")
                break

            # Inject synthetic sudden drift for evaluation
            if i >= DRIFT_START:
                data["Amount"] = data["Amount"] * DRIFT_SCALE
                data["drift_injected"] = True
            else:
                data["drift_injected"] = False

            producer.send(topic, value=data)
            if i % 100 == 0 and i > 0:
                logger.info("[SEND] Sent %d records...", i)
                if metrics_queue is not None:
                    metrics_queue.put(i)
            time.sleep(SLEEP_SECONDS)

        producer.flush()
        if metrics_queue is not None:
            metrics_queue.put(len(records))
        logger.info(" Producer finished.")
    except Exception as e:
        logger.error("Producer error: %s", e, exc_info=True)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)
    run_producer()

