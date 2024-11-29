import os
import sys
import signal
import threading

from loguru import logger

from models.consumer_cursor import ConsumerCursorManager

if __name__ == "__main__":
    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info(
            "Shutdown signal received. Waiting for current processing to complete."
        )
        terminate_event.set()


    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


    def patch_record(record):
        record["extra"]["service"] = 'balance-indexer-consumer'
        return True


    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    logger.add(
        "logs/balance-tracking-consumer.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    db_url = os.getenv(
        "REDPANDA_DB_CONNECTION_STRING",
        "postgresql://postgres:changeit456$@localhost:5420/redpanda"
    )
    redpanda_bootstrap_servers = os.getenv(
        "REDPANDA_BOOTSTRAP_SERVERS",
        "localhost:19092"
    )

    consumer_cursor_manager = ConsumerCursorManager(db_url)

    kafka_config = {
        'bootstrap.servers': redpanda_bootstrap_servers,
        'group.id': 'balance-indexer-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
    }

    try:
        logger.info("Starting balance indexer consumer")
        consumer = BalanceIndexerConsumer(
            kafka_config,
            consumer_state_manager,
            terminate_event
        )
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        consumer_state_manager.close()
        logger.info("Balance indexer consumer stopped")