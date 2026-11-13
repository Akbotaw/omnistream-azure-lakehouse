"""
Bronze Layer CDC Ingestion Pipeline.
Reads raw Debezium CDC events from Apache Kafka / Azure Event Hubs using
PySpark Structured Streaming and appends them into Bronze Delta Lake tables.
"""

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

TABLES = ["listings", "bookings", "payments"]

def transform_kafka_to_bronze(
    kafka_df: DataFrame, source_table: str
) -> DataFrame:
    """Cast binary Kafka key/value payloads to strings and attach audit metadata."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.lit(source_table).alias("source_table"),
        F.current_timestamp().alias("_ingested_at"),
    )

def ingest_topic_to_bronze(
    spark: SparkSession,
    bootstrap_servers: str,
    topic_prefix: str,
    source_table: str,
    bronze_base_path: str,
) -> None:
    """Stream a single CDC topic from Kafka into a Bronze Delta table."""
    topic_name = f"{topic_prefix}.airbnb.{source_table}"
    table_path = f"{bronze_base_path}/tables/{source_table}_cdc"
    checkpoint_path = f"{bronze_base_path}/checkpoints/{source_table}_cdc"

    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = transform_kafka_to_bronze(raw_kafka_df, source_table)

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(table_path)
    )
    query.awaitTermination()
    print(f"[BRONZE] Successfully ingested {topic_name} -> {table_path}")

if __name__ == "__main__":
    spark = get_spark_session("OmniStream-Bronze-CDC-Ingest")
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic_prefix = os.environ.get("KAFKA_TOPIC_PREFIX", "omnistream")
    bronze_path = os.environ.get("BRONZE_BASE_PATH", "data/bronze")

    for table in TABLES:
        ingest_topic_to_bronze(
            spark = spark,
            bootstrap_servers = kafka_servers,
            topic_prefix = topic_prefix,
            source_table = table,
            bronze_base_path = bronze_path
        )
