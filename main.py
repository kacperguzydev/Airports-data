import os
from kaggle_downloader import download_file, CSV_FILENAME, DOWNLOAD_DIR
from kafka_producer import produce_to_kafka
from loguru import logger
import postgresql_database
from tracing import init_tracer

tracer = init_tracer()

def main():
    with tracer.start_as_current_span("main_pipeline"):
        logger.info("Starting data pipeline: create DB & table, download CSV, then send data to Kafka.")

        # Create the PostgreSQL database (if it doesn't exist)
        postgresql_database.create_database()

        # Create the required table in PostgreSQL
        postgresql_database.create_table()

        # Download the CSV file from Kaggle
        download_file()

        # Build and check the CSV file path
        csv_file_path = os.path.join(DOWNLOAD_DIR, CSV_FILENAME)
        if not os.path.exists(csv_file_path):
            logger.error("CSV file not found at: {}", csv_file_path)
            return

        # Send the CSV data to Kafka
        produce_to_kafka(csv_file_path)
        logger.info("Data pipeline completed successfully.")

if __name__ == "__main__":
    main()
