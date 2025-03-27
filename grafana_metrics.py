import time
import psycopg2
import config
import schedule
from prometheus_client import start_http_server, Gauge
from loguru import logger

airport_country_count = Gauge(
    "airport_country_count",
    "Number of airports in each country",
    ["country"]
)

def fetch_airport_data():
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        query = """
            SELECT country, COUNT(*) AS count
            FROM airports_lookup
            GROUP BY country;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.error("Failed to fetch airport data: {}", e)
        return []

def update_metrics():
    rows = fetch_airport_data()
    airport_country_count.clear()
    for row in rows:
        country = row[0] if row[0] else "Unknown"
        count = row[1]
        airport_country_count.labels(country=country).set(count)
    logger.info("Updated airport_country_count metric with {} entries.", len(rows))

def main():
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000.")
    schedule.every(60).seconds.do(update_metrics)
    logger.info("Scheduled metrics update every 60 seconds.")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
