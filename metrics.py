from prometheus_client import Counter, Histogram

# Count how many messages have been sent to Kafka.
MESSAGES_SENT = Counter("messages_sent_total", "Total number of messages sent to Kafka")

# Histogram to record the time spent upserting records to PostgreSQL.
DB_UPSERT_TIME = Histogram("db_upsert_time_seconds", "Time spent upserting records to PostgreSQL")

# Histogram for the processing time of each Spark micro-batch.
BATCH_PROCESSING_TIME = Histogram("batch_processing_time_seconds", "Time taken to process each micro-batch")
