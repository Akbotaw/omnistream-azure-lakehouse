{{ config(materialized='table') }}
SELECT
    city,
    COUNT(booking_id) AS total_bookings,
    SUM(CASE WHEN booking_status = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_bookings,
    SUM(CASE WHEN booking_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_bookings,
    SUM(CASE WHEN booking_status = 'CONFIRMED' THEN total_amount_usd ELSE 0 END) AS confirmed_revenue_usd
FROM {{ ref('fct_bookings') }}
GROUP BY city
