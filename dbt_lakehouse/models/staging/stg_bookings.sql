{{ config(materialized='view') }}

SELECT
    booking_id,
    listing_id,
    guest_id,
    guest_country,
    booking_status,
    DATE '1970-01-01' + CAST(check_in_date AS INTEGER) AS check_in_date,
    DATE '1970-01-01' + CAST(check_out_date AS INTEGER) AS check_out_date,
    nights,
    total_amount,
    currency
FROM delta_scan('data/silver/tables/bookings')
