"""Unit tests for Bronze CDC transformation logic using pytest and chispa."""

from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from src.bronze.ingest_cdc import transform_kafka_to_bronze

def test_transform_kafka_to_bronze(spark: SparkSession) -> None:
    # 1. ARRANGE: Create fake Kafka input DataFrame in memory (with binary bytes b'...')
    input_data = [
        (
            b'{"listing_id": 2595}',
            b'{"op": "c", "after": {"listing_id": 2595, "city": "New York"}}',
            "omnistream.airbnb.listings",
            0,
            10,
            datetime(2026, 11, 12, 10, 0, 0),
        )
    ]

    input_df = spark.createDataFrame(
        input_data,
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )

    # 2. ACT: Run your transformation function (drop current_timestamp column to compare exact values)
    actual_df = transform_kafka_to_bronze(input_df, source_table="listings").drop("_ingested_at")


    #3. ASSERT: Define what the output DataFrame MUST look like
    expected_data = [
        (
            '{"listing_id": 2595}',
            '{"op": "c", "after": {"listing_id": 2595, "city": "New York"}}',
            "omnistream.airbnb.listings",
            0,
            10,
            datetime(2026, 11, 12, 10, 0, 0),
            "listings",
        )
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            "kafka_key",
            "raw_payload",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "source_table",
        ],
    )

    # Compare actual vs expected using chispa
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)