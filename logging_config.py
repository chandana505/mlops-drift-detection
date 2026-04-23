import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


class _KafkaNoiseFilter(logging.Filter):
    """Suppress kafka connection heartbeat logs below WARNING."""
    def filter(self, record):
        return not record.name.startswith("kafka.")


def setup_logging():
    """Configure dual logging: console + rotating file in logs/."""
    logs_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"run_{timestamp}.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Rotating file handler — 10 MB per file, keep last 5 backups, force UTF-8
    file_handler = RotatingFileHandler(
        log_file, mode="a", maxBytes=10_485_760, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(_KafkaNoiseFilter())

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(_KafkaNoiseFilter())

    # Root logger config
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if setup_logging is called multiple times
    if not any(isinstance(h, (logging.FileHandler, RotatingFileHandler)) for h in root_logger.handlers):
        root_logger.addHandler(file_handler)
    if not any(type(h) is logging.StreamHandler for h in root_logger.handlers):
        root_logger.addHandler(console_handler)

    logging.info("Logging initialized. Log file: %s", log_file)
    return log_file

