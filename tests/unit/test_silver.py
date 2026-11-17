"""Unit tests for Silver CDC processor covering all 5 CDC edge cases."""
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.silver.cdc_processor import (
    BOOKINGS_SCHEMA,
    LISTINGS_SCHEMA,
    build_scd2_history,
    deduplicate_cdc_batch,
    parse_debezium_cdc,
    split_valid_and_quarantine
)

def test_parse_and_quarantine_preserves_delete_and_routes_corrupt_json(
    spark: SparkSession,
) -> None:
    # 1. ARRANGE:
    # Row 1: DELETE event (after is null, booking_id=99 only in before)
    # Row 2: Corrupted non-JSON payload -> should go to quarantine
    bronze_data = [
        (
            '{"before": {"booking_id": 99, "listing_id": 2595, "guest_id": 1, "guest_country": "US", '
            '"booking_status": "CANCELLED", "check_in_date": "2026-12-01", "check_out_date": "2026-12-05", '
            '"nights": 4, "total_amount": 500.0, "currency": "USD"}, '
            '"after": null, "source": {"lsn": 900, "table": "bookings"}, "op": "d", "ts_ms": 1731140009000}',
        ),
        ("CORRUPTED_JSON_PAYLOAD_502_ERROR",),
    ]
    bronze_df = spark.createDataFrame(bronze_data, ["raw_payload"])

    # 2. ACT
    parsed_df = parse_debezium_cdc(bronze_df, BOOKINGS_SCHEMA)
    valid_df, quarantine_df = split_valid_and_quarantine(parsed_df, "booking_id")

    # 3. ASSERT
    valid_rows = valid_df.collect()
    assert len(valid_rows) == 1
    assert valid_rows[0]["booking_id"] == 99
    assert valid_rows[0]["op"] == "d"

    quarantine_rows = quarantine_df.collect()
    assert len(quarantine_rows) == 1
    assert quarantine_rows[0]["raw_payload"] == "CORRUPTED_JSON_PAYLOAD_502_ERROR"


def test_deduplicate_cdc_batch_picks_highest_lsn(spark: SparkSession) -> None:
    # 1. ARRANGE: 3 changes for the same booking_id=101 in one batch
    input_data = [
        (101, "PENDING", "c", 500, 1000),
        (101, "CONFIRMED", "u", 550, 2000),
        (101, "CANCELLED", "u", 620, 3000),
    ]
    input_df = spark.createDataFrame(
        input_data, ["booking_id", "booking_status", "op", "lsn", "ts_ms"]
    )
     # 2. ACT
    actual_df = deduplicate_cdc_batch(input_df, "booking_id")
    # 3. ASSERT with chispa: only the row with lsn=620 must remain!
    expected_data = [
        (101, "CANCELLED", "u", 620, 3000),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["booking_id", "booking_status", "op", "lsn", "ts_ms"]
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_build_scd2_history_creates_valid_from_and_valid_to(
    spark: SparkSession,
) -> None:
   # Listing 2595 changes price from 225.0 (lsn=10) to 280.0 (lsn=20), plus a network duplicate of lsn=10
    data = [
        (2595, 225.0, "c", 10, 1700000000000),
        (2595, 225.0, "c", 10, 1700000000000),  # duplicate of lsn=10
        (2595, 280.0, "u", 20, 1700003600000),
    ]
    df = spark.createDataFrame(
        data, ["listing_id", "price_per_night", "op", "lsn", "ts_ms"]
    )
   
    scd2_df = build_scd2_history(df, "listing_id").orderBy("lsn")
    rows = scd2_df.collect()

    assert len(rows) == 2
    assert rows[0]["price_per_night"] == 225.0
    assert rows[0]["valid_to"] == rows[1]["valid_from"]
    assert rows[1]["price_per_night"] == 280.0
    assert rows[1]["valid_to"] is None
