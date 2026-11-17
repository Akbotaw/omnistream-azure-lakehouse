"""
Silver Layer CDC Processor.
Handles all 5 production CDC challenges:
1. Envelope parsing & DELETE key preservation via COALESCE(after, before)
2. Quarantine (Dead Letter Queue) routing for malformed/null-PK events
3. In-batch deduplication by primary key ordered by WAL LSN descending
4. Out-of-order protection during Delta Lake MERGE (source.lsn >= target.lsn)
5. SCD Type 2 historical interval construction (valid_from, valid_to)
"""

import os 
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.utils.spark_session import get_spark_session

# Schemas for 3 Airbnb tables
LISTINGS_SCHEMA = T.StructType(
    [
        T.StructField("listing_id", T.IntegerType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("host_id", T.IntegerType(), True),
        T.StructField("host_name", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("neighbourhood", T.StringType(), True),
        T.StructField("room_type", T.StringType(), True),
        T.StructField("price_per_night", T.DoubleType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("minimum_nights", T.IntegerType(), True),
        T.StructField("listing_status", T.StringType(), True),
    ]
)
BOOKINGS_SCHEMA = T.StructType(
    [
        T.StructField("booking_id", T.IntegerType(), True),
        T.StructField("listing_id", T.IntegerType(), True),
        T.StructField("guest_id", T.IntegerType(), True),
        T.StructField("guest_country", T.StringType(), True),
        T.StructField("booking_status", T.StringType(), True),
        T.StructField("check_in_date", T.StringType(), True),
        T.StructField("check_out_date", T.StringType(), True),
        T.StructField("nights", T.IntegerType(), True),
        T.StructField("total_amount", T.DoubleType(), True),
        T.StructField("currency", T.StringType(), True),
    ]
)
PAYMENTS_SCHEMA = T.StructType(
    [
        T.StructField("payment_id", T.IntegerType(), True),
        T.StructField("booking_id", T.IntegerType(), True),
        T.StructField("payment_method", T.StringType(), True),
        T.StructField("payment_status", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("currency", T.StringType(), True),
    ]
)

def build_envelope_schema(record_schema: T.StructType)-> T.StructType:
    """Wrap a table's row schema inside the standard Debezium CDC envelope schema."""

    source_schema = T.StructType([
        T.StructField("lsn", T.LongType(), True),
        T.StructField("table", T.LongType(), True),
    ])
    return T.StructType(
        [
            T.StructField("before", record_schema, True),
            T.StructField("after", record_schema, True),
            T.StructField("source", source_schema, True),
            T.StructField("op", T.StringType(), True),
            T.StructField("ts_ms", T.LongType(), True),
        ]
    ) 

def parse_debezium_cdc(
    bronze_df: DataFrame, record_schema: T.StructType
) -> DataFrame:
    """Parse raw Debezium JSON and extract columns using COALESCE(after, before) to preserve DELETE keys."""
    envelope_schema = build_envelope_schema(record_schema)
    parsed_df = bronze_df.withColumn(
        "cdc", F.from_json(F.col("raw_payload"), envelope_schema)
    )

    extracted_cols = [
        F.coalesce(
            F.col(f"cdc.after.{col_name}"), F.col(f"cdc.before.{col_name}")
        ).alias(col_name)
        for col_name in record_schema.fieldNames()
    ]

    return parsed_df.select(
        *extracted_cols,
        F.col("cdc.op").alias("op"),
        F.col("cdc.source.lsn").alias("lsn"),
        F.col("cdc.ts_ms").alias("ts_ms"),
        F.col("raw_payload"),
    )

def split_valid_and_quarantine(
    parsed_df: DataFrame, primary_key: str
) -> tuple[DataFrame, DataFrame]:
    """Route malformed JSON or null primary key records to quarantine (Dead Letter Queue)."""
    is_valid = (
        F.col(primary_key).isNotNull()
        & F.col("op").isin("c", "u", "d", "r")
        & F.col("lsn").isNotNull()
    )
    valid_df = parsed_df.filter(is_valid).drop("raw_payload")
    quarantine_df = parsed_df.filter(~is_valid).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return valid_df, quarantine_df

def deduplicate_cdc_batch(df: DataFrame, primary_key: str) -> DataFrame:
    """Keep only the latest CDC event per primary_key ordered by LSN and timestamp descending."""
    window_spec = Window.partitionBy(primary_key).orderBy(
       F.col("lsn").desc(), F.col("ts_ms").desc()
    )
    ranked_df = df.withColumn("_rn", F.row_number().over(window_spec))
    return ranked_df.filter(F.col("_rn") == 1).drop("_rn")

def build_scd2_history(df: DataFrame, primary_key: str) -> DataFrame:
    """Build SCD Type 2 version intervals (valid_from, valid_to) ordered by LSN ascending."""
    # First remove technical network duplicates having identical (primary_key, lsn)
    tech_dedup_win = Window.partitionBy(primary_key, "lsn").orderBy(
         F.col("ts_ms").desc()
    )
    unique_events_df = (
        df.withColumn("_tech_rn", F.row_number().over(tech_dedup_win))
        .filter(F.col("_tech_rn") == 1)
        .drop("_tech_rn")
    )

    # Order versions chronologically by LSN to compute valid_from and valid_to
    history_win = Window.partitionBy(primary_key).orderBy(
        F.col("lsn").asc(), F.col("ts_ms").asc()
    )
    return (
        unique_events_df.withColumn("valid_from", (F.col("ts_ms") / 1000).cast("timestamp")
        )
        .withColumn(
            "valid_to",
            F.lead((F.col("ts_ms") / 1000).cast("timestamp")).over(history_win),
        )
    )

def merge_cdc_to_silver(
    spark: SparkSession,
    deduped_df: DataFrame,
    silver_table_path: str,
    primary_key: str,
) -> None:
    """Upsert and delete records in Silver Delta table with out-of-order LSN protection."""
    if not DeltaTable.isDeltaTable(spark, silver_table_path):
        # First load: insert only non-delete records and enable Change Data Feed
        initial_df = deduped_df.filter(F.col("op") != "d")
        (
            initial_df.write.format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .save(silver_table_path)
        )
        return
    delta_table = DeltaTable.forPath(spark, silver_table_path)
    (
        delta_table.alias("target")
        .merge(
            source=deduped_df.alias("source"),
            condition=f"target.{primary_key} = source.{primary_key}",
        )
        .whenMatchedDelete(
            condition="source.op = 'd' AND source.lsn >= target.lsn"
        )
        .whenMatchedUpdateAll(
            condition="source.op != 'd' AND source.lsn >= target.lsn"
        )
        .whenNotMatchedInsertAll(condition="source.op != 'd'")
        .execute()
    )

def process_bronze_to_silver_table(
    spark: SparkSession,
    source_table: str,
    primary_key: str,
    record_schema: T.StructType,
    bronze_base_path: str = "data/bronze",
    silver_base_path: str = "data/silver",
) -> None:
    """End-to-end Bronze to Silver processor for a given CDC table."""
    bronze_path = f"{bronze_base_path}/tables/{source_table}_cdc"
    silver_path = f"{silver_base_path}/tables/{source_table}"
    quarantine_path = f"{silver_base_path}/quarantine/{source_table}"

    bronze_df = spark.read.format("delta").load(bronze_path)
    parsed_df = parse_debezium_cdc(bronze_df, record_schema)
    valid_df, quarantine_df = split_valid_and_quarantine(parsed_df, primary_key)

    if not quarantine_df.isEmpty():
        quarantine_df.write.format("delta").mode("append").save(quarantine_path)

    if source_table == "listings":
      # Save listings as SCD Type 2 history table (single source of truth)
      scd2_df = build_scd2_history(valid_df, primary_key)
      scd2_df.write.format("delta").mode("overwrite").save(silver_path)
      print(
          f"[SILVER SCD2] Successfully built {source_table} history ->"
          f" {silver_path}"
      )
    else:
      # Save transactional tables (bookings, payments) as SCD Type 1 via MERGE
      deduped_df = deduplicate_cdc_batch(valid_df, primary_key)
      merge_cdc_to_silver(spark, deduped_df, silver_path, primary_key)
      print(
          f"[SILVER SCD1] Successfully merged {source_table} -> {silver_path}"
      )

if __name__ =="__main__":
    spark = get_spark_session("OmniStream-Silver-CDC-Processor")
    bronze_base = os.environ.get("BRONZE_BASE_PATH", "data/bronze")
    silver_base = os.environ.get("SILVER_BASE_PATH", "data/silver")

    table_configs = [
        ("listings", "listing_id", LISTINGS_SCHEMA),
        ("bookings", "booking_id", BOOKINGS_SCHEMA),
        ("payments", "payment_id", PAYMENTS_SCHEMA),
    ]

    for tbl_name, pk, schema in table_configs:
        process_bronze_to_silver_table(
            spark=spark,
            source_table=tbl_name,
            primary_key=pk,
            record_schema=schema,
            bronze_base_path=bronze_base,
            silver_base_path=silver_base,
        )