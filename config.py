import os

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi_topic")  # Update if needed

# PostgreSQL settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "airports_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Limit maximum records to process from CSV
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "10000"))

# OpenTelemetry/Jaeger tracing settings
OTEL_EXPORTER_JAEGER_AGENT_HOST = os.getenv("OTEL_EXPORTER_JAEGER_AGENT_HOST", "localhost")
OTEL_EXPORTER_JAEGER_AGENT_PORT = int(os.getenv("OTEL_EXPORTER_JAEGER_AGENT_PORT", "6831"))
