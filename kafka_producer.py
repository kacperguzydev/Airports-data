import json
import csv
import time
from kafka import KafkaProducer
import config
from loguru import logger
from tracing import init_tracer
from metrics import MESSAGES_SENT

tracer = init_tracer()

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def key_serializer(key):
    return key.encode("utf-8")

def produce_to_kafka(csv_file_path):
    with tracer.start_as_current_span("produce_to_kafka"):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKER,
                value_serializer=json_serializer,
                key_serializer=key_serializer
            )
        except Exception as err:
            logger.error("Failed to initialize Kafka producer: {}", err)
            return

        try:
            with open(csv_file_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                airport_data = list(reader)
        except Exception as err:
            logger.error("Failed to read CSV file {}: {}", csv_file_path, err)
            return

        if not airport_data:
            logger.warning("No airport data found in CSV file.")
            return

        total_records = len(airport_data)
        if total_records > config.MAX_RECORDS:
            logger.warning(
                "Record count ({}) exceeds the maximum limit ({}). Limiting to {} records.",
                total_records, config.MAX_RECORDS, config.MAX_RECORDS
            )
            airport_data = airport_data[:config.MAX_RECORDS]

        logger.info("Processing {} records from CSV.", len(airport_data))

        futures = []
        for row in airport_data:
            message_key = row.get("Airport ID", "unknown")
            try:
                future = producer.send(config.KAFKA_TOPIC, key=str(message_key), value=row)
                futures.append(future)
                MESSAGES_SENT.inc()
            except Exception as err:
                logger.error("Failed to send message for key {}: {}", message_key, err)

        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                logger.error("Error waiting for message send: {}", e)

        try:
            producer.flush()
            logger.info("All messages have been sent to Kafka.")
        except Exception as err:
            logger.error("Error flushing Kafka producer: {}", err)

if __name__ == "__main__":
    produce_to_kafka("data/airports.csv")
