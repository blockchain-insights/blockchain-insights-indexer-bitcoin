import os
import time
import signal
import sys
import threading
from queue import Queue, Empty
from loguru import logger
from node.storage import Storage


class TransactionVoutIndexer:
    def __init__(self, connection_params, terminate_event):
        """Initialize the Transaction Vout Indexer.

        Args:
            connection_params: Database connection parameters
            terminate_event: Event for handling graceful shutdown
        """
        self.connection_params = connection_params
        self.terminate_event = terminate_event
        self.completed_chunks = Queue()
        # Main storage instance for non-threaded operations
        self.storage = Storage(connection_params)

    @staticmethod
    def calculate_chunk_positions(csv_file, n_threads=64):
        """Calculate the start and end positions for each chunk of the CSV file.

        Args:
            csv_file (str): Path to the CSV file
            n_threads (int): Number of threads to split the file into

        Returns:
            list: List of tuples containing (start_pos, end_pos) for each chunk
        """
        positions = []

        with open(csv_file, 'r', newline='') as file:
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            chunk_size = file_size // n_threads

            for i in range(n_threads):
                pos = i * chunk_size
                if pos == 0:
                    positions.append(pos)
                else:
                    file.seek(pos)
                    file.readline()
                    positions.append(file.tell())

        result = []
        for i in range(len(positions)):
            start_pos = positions[i]
            end_pos = positions[i + 1] if i < len(positions) - 1 else -1
            result.append((start_pos, end_pos))
        return result

    def process_chunk(self, chunk_id, pos_range, csv_file):
        """Process a chunk of lines from the CSV file.

        Args:
            chunk_id (int): Identifier for this chunk
            pos_range (tuple): Tuple of (start_pos, end_pos) for the chunk
            csv_file (str): Path to the CSV file
        """
        # Create a new storage instance for this thread
        thread_storage = Storage(self.connection_params)
        start_pos, end_pos = pos_range
        records = []
        batch_size = 10000
        total_processed = 0

        try:
            logger.debug(f"Thread {chunk_id} starting", start_pos=start_pos, end_pos=end_pos)
            with open(csv_file, 'r') as file:
                file.seek(start_pos)
                while not self.terminate_event.is_set():
                    line = file.readline()
                    if not line:
                        break

                    columns = line.split(';')
                    txid = columns[0].strip()
                    vout = columns[1].strip()
                    value = columns[2].strip()
                    address = columns[4].strip()
                    records.append((txid, vout, value, address))
                    total_processed += 1

                    if len(records) >= batch_size:
                        try:
                            thread_storage.batch_insert(records)
                            logger.debug(f"Thread {chunk_id} inserted batch",
                                       records=len(records),
                                       total_processed=total_processed)
                            records = []
                        except Exception as e:
                            logger.error(f"Thread {chunk_id} error during batch insert: {str(e)}")
                            self.terminate_event.set()
                            return

                    if end_pos > -1 and file.tell() >= end_pos:
                        break

            if records and not self.terminate_event.is_set():
                try:
                    thread_storage.batch_insert(records)
                    total_processed += len(records)
                    logger.debug(f"Thread {chunk_id} inserted final batch",
                               records=len(records),
                               total_processed=total_processed)
                except Exception as e:
                    logger.error(f"Thread {chunk_id} error during final batch insert: {str(e)}")
                    self.terminate_event.set()
                    return

            logger.info(f"Thread {chunk_id} completed", total_processed=total_processed)
            self.completed_chunks.put(chunk_id)

        except Exception as e:
            logger.error(f"Thread {chunk_id} encountered error: {str(e)}")
            self.terminate_event.set()

    def run(self, csv_file, n_threads=64):
        """Run the indexing process.

        Args:
            csv_file (str): Path to the CSV file
            n_threads (int): Number of threads to use for processing
        """
        logger.info(f"Indexing {csv_file} with {n_threads} threads.")
        start_time = time.time()

        try:
            chunk_positions = self.calculate_chunk_positions(csv_file, n_threads)
            threads = []

            logger.info("Starting worker threads...")
            logger.debug("Chunk positions", chunk_positions=chunk_positions)

            # Create and start threads
            for i, chunk in enumerate(chunk_positions):
                thread = threading.Thread(
                    target=self.process_chunk,
                    args=(i, chunk, csv_file)
                )
                thread.start()
                threads.append(thread)

            # Wait for completion or termination
            completed_threads = 0
            while completed_threads < len(threads) and not self.terminate_event.is_set():
                try:
                    # Check for completed chunks
                    chunk_id = self.completed_chunks.get(timeout=0.1)
                    completed_threads += 1
                    logger.debug(f"Thread {chunk_id} completed",
                               completed=completed_threads,
                               total=len(threads))
                except Empty:
                    continue

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

            end_time = time.time()

            if self.terminate_event.is_set():
                logger.warning("Processing terminated before completion")
            else:
                logger.info(f"Indexing completed in {end_time - start_time} seconds.")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            self.terminate_event.set()
            raise e


if __name__ == '__main__':
    import argparse
    from dotenv import load_dotenv

    load_dotenv()
    # --csvpath /mnt/c/bitcoin_data/tx_out-0-99999.csv --threads 2
    # --csvpath /mnt/c/bitcoin_data/tx_out-200000-299999.csv --threads 2

    # --csvpath /mnt/c/bitcoin_data/tx_out-300000-399999.csv --threads 3
    # --csvpath /mnt/c/bitcoin_data/tx_out-400000-499999.csv --threads 4

    parser = argparse.ArgumentParser(description='Construct a hash table from vout csv data.')
    parser.add_argument('--csvpath', required=True, type=str, help='Path to the CSV file')
    parser.add_argument('--threads', type=int, default=64, help='Number of threads to use')
    parser.add_argument('--cleanup', action='store_true', help='Remove duplicates from the database')
    args = parser.parse_args()

    service_name = 'transaction-vout-indexer'

    def patch_record(record):
        record["extra"]["service"] = service_name
        return True

    logger.remove()
    logger.add(
        f"../../logs/{service_name}.log",
        rotation="500 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} | {extra}",
        level="DEBUG",
        filter=patch_record
    )

    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <blue>{message}</blue> | {extra}",
        level="DEBUG",
        filter=patch_record,
    )

    if not os.path.isfile(args.csvpath):
        logger.error("CSV file not found")
        sys.exit(1)

    terminate_event = threading.Event()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Waiting for current processing to complete...")
        terminate_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    connection_params = {
        "host": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_HOST", "localhost"),
        "port": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PORT", "8123"),
        "database": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_DATABASE", "transaction_stream"),
        "user": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_USER", "default"),
        "password": os.getenv("TRANSACTION_STREAM_CLICKHOUSE_PASSWORD", "changeit456$"),
        "max_execution_time": int(os.getenv("TRANSACTION_STREAM_CLICKHOUSE_MAX_EXECUTION_TIME", "3600")),
    }

    if args.cleanup:
        logger.info("Removing duplicates from the database...")
        storage = Storage(connection_params)
        storage.remove_duplicates()
        logger.info("Duplicates removed.")
        sys.exit(0)


    # Create indexer with connection params instead of Storage instance
    indexer = TransactionVoutIndexer(connection_params, terminate_event)
    indexer.run(args.csvpath, args.threads)

    logger.info("Transaction Vout Indexer finished.")