import os
import psycopg2
from psycopg2 import sql
from loguru import logger
import config
from cerberus import Validator
from tracing import init_tracer
from metrics import DB_UPSERT_TIME

tracer = init_tracer()

airport_schema = {
    "airport_id":             {"type": "integer", "required": True},
    "name":                   {"type": "string",  "required": True,  "empty": False},
    "city":                   {"type": "string",  "required": False, "empty": True},
    "country":                {"type": "string",  "required": False, "empty": True},
    "iata":                   {"type": "string",  "required": False, "empty": True},
    "icao":                   {"type": "string",  "required": False, "empty": True},
    "latitude":               {"type": "float",   "required": False},
    "longitude":              {"type": "float",   "required": False},
    "altitude":               {"type": "integer", "required": False},
    "timezone":               {"type": "string",  "required": False, "empty": True},
    "dst":                    {"type": "string",  "required": False, "empty": True},
    "database_timezone_type": {"type": "string",  "required": False, "empty": True},
    "source":                 {"type": "string",  "required": False, "empty": True}
}
validator = Validator(airport_schema)

def create_database():
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database="postgres",
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.DB_NAME,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config.DB_NAME)))
            logger.info("Database '{}' created successfully.", config.DB_NAME)
        else:
            logger.info("Database '{}' already exists.", config.DB_NAME)
        cur.close()
        conn.close()
    except Exception as err:
        logger.error("Error creating database: {}", err)
        raise

def get_connection():
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        logger.info("Connected to PostgreSQL database.")
        return conn
    except Exception as err:
        logger.error("Error connecting to PostgreSQL: {}", err)
        raise

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        table_sql = """
            CREATE TABLE IF NOT EXISTS airports_lookup (
                airport_id INT PRIMARY KEY,
                name VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                iata VARCHAR(10),
                icao VARCHAR(10),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                altitude INT,
                timezone VARCHAR(50),
                dst VARCHAR(10),
                database_timezone_type VARCHAR(50),
                source VARCHAR(255)
            );
        """
        cur.execute(table_sql)
        conn.commit()
        logger.info("Table 'airports_lookup' created or already exists.")
    except Exception as err:
        conn.rollback()
        logger.error("Error creating table: {}", err)
    finally:
        cur.close()
        conn.close()

def insert_airport_record(record):
    if not validator.validate(record):
        logger.error("Invalid record {}: errors: {}", record, validator.errors)
        return

    conn = get_connection()
    cur = conn.cursor()
    with tracer.start_as_current_span("insert_airport_record"), DB_UPSERT_TIME.time():
        try:
            sql_query = """
                INSERT INTO airports_lookup (
                    airport_id, name, city, country, iata, icao,
                    latitude, longitude, altitude, timezone,
                    dst, database_timezone_type, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (airport_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    iata = EXCLUDED.iata,
                    icao = EXCLUDED.icao,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    altitude = EXCLUDED.altitude,
                    timezone = EXCLUDED.timezone,
                    dst = EXCLUDED.dst,
                    database_timezone_type = EXCLUDED.database_timezone_type,
                    source = EXCLUDED.source;
            """
            cur.execute(sql_query, (
                record.get("airport_id"),
                record.get("name"),
                record.get("city"),
                record.get("country"),
                record.get("iata"),
                record.get("icao"),
                record.get("latitude"),
                record.get("longitude"),
                record.get("altitude"),
                record.get("timezone"),
                record.get("dst"),
                record.get("database_timezone_type"),
                record.get("source")
            ))
            conn.commit()
            logger.info("Upserted record with airport_id: {}", record.get("airport_id"))
        except Exception as err:
            conn.rollback()
            logger.error("Error upserting record {}: {}", record, err)
        finally:
            cur.close()
            conn.close()

def insert_airport_records(records):
    valid_records = []
    for rec in records:
        if validator.validate(rec):
            valid_records.append(rec)
        else:
            logger.error("Invalid record {}: errors: {}", rec, validator.errors)

    if not valid_records:
        logger.warning("No valid airport records to insert.")
        return

    conn = get_connection()
    cur = conn.cursor()
    with tracer.start_as_current_span("insert_airport_records"), DB_UPSERT_TIME.time():
        try:
            sql_query = """
                INSERT INTO airports_lookup (
                    airport_id, name, city, country, iata, icao,
                    latitude, longitude, altitude, timezone,
                    dst, database_timezone_type, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (airport_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    iata = EXCLUDED.iata,
                    icao = EXCLUDED.icao,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    altitude = EXCLUDED.altitude,
                    timezone = EXCLUDED.timezone,
                    dst = EXCLUDED.dst,
                    database_timezone_type = EXCLUDED.database_timezone_type,
                    source = EXCLUDED.source;
            """
            data = [
                (
                    rec.get("airport_id"),
                    rec.get("name"),
                    rec.get("city"),
                    rec.get("country"),
                    rec.get("iata"),
                    rec.get("icao"),
                    rec.get("latitude"),
                    rec.get("longitude"),
                    rec.get("altitude"),
                    rec.get("timezone"),
                    rec.get("dst"),
                    rec.get("database_timezone_type"),
                    rec.get("source")
                )
                for rec in valid_records
            ]
            cur.executemany(sql_query, data)
            conn.commit()
            logger.info("Upserted {} valid airport records.", len(valid_records))
        except Exception as err:
            conn.rollback()
            logger.error("Error upserting records: {}", err)
        finally:
            cur.close()
            conn.close()

if __name__ == "__main__":
    create_database()
    create_table()
