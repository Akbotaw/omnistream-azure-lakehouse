{{ config(materialized='table') }}
SELECT
    b.booking_id,
    b.listing_id,
    l.listing_name,
    l.city,
    l.room_type,
    b.guest_id,
    b.guest_country,
    b.booking_status,
    b.check_in_date,
    b.check_out_date,
    b.nights,
    b.total_amount AS original_amount,
    b.currency AS original_currency,
    ROUND(b.total_amount * COALESCE(fx.rate_to_usd, 1.0), 2) AS total_amount_usd,
    COALESCE(p.payment_status, 'UNPAID') AS payment_status
FROM {{ ref('stg_bookings') }} AS b
LEFT JOIN {{ ref('dim_listings') }} AS l
    ON b.listing_id = l.listing_id
LEFT JOIN {{ ref('stg_payments') }} AS p
    ON b.booking_id = p.booking_id
LEFT JOIN {{ ref('exchange_rates') }} AS fx
    ON b.currency = fx.currency
