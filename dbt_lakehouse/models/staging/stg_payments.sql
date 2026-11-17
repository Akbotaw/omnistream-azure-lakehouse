{{ config(materialized='view') }}
SELECT
    payment_id,
    booking_id,
    payment_method,
    payment_status,
    amount AS payment_amount,
    currency AS payment_currency
FROM delta_scan('data/silver/tables/payments')
