import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import config
from loguru import logger
import postgresql_database
from tracing import init_tracer
from metrics import BATCH_PROCESSING_TIME

tracer = init_tracer()

os.environ["HADOOP_HOME"] = "C:\\hadoop"

def process_batch(df, batch_id):
    with tracer.start_as_current_span("process_batch"):
        with BATCH_PROCESSING_TIME.time():
            try:
                records = [row.asDict() for row in df.collect()]
                postgresql_database.insert_airport_records(records)
                logger.info("Batch {} processed: {} records upserted.", batch_id, len(records))
            except Exception as err:
                logger.error("Error processing batch {}: {}", batch_id, err)

def main():
    spark = (
        SparkSession.builder.appName("AirportsKafkaConsumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.18"
        )
        .getOrCreate()
    )

    airport_schema = StructType([
        StructField("Airport ID", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("IATA", StringType(), True),
        StructField("ICAO", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), True),
        StructField("Altitude", StringType(), True),
        StructField("Timezone", StringType(), True),
        StructField("DST", StringType(), True),
        StructField("Database Timezone", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("Source", StringType(), True)
    ])

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = json_df.select(F.from_json(F.col("json_str"), airport_schema).alias("data")).select("data.*")
    parsed_df = parsed_df.drop("Database Timezone", "Type")

    airports_df = (
        parsed_df
        .withColumnRenamed("Airport ID", "airport_id")
        .withColumnRenamed("Name", "name")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("IATA", "iata")
        .withColumnRenamed("ICAO", "icao")
        .withColumn("latitude", F.col("Latitude").cast("float"))
        .withColumn("longitude", F.col("Longitude").cast("float"))
        .withColumn("altitude", F.col("Altitude").cast("int"))
        .withColumn("timezone", F.col("Timezone").cast("string"))
        .withColumnRenamed("DST", "dst")
        .withColumnRenamed("Source", "source")
        .withColumn("airport_id", F.col("airport_id").cast("int"))
    )

    query = (
        airports_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/airports_checkpoint")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
