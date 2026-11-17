{{ config(materialized='view') }}
SELECT
    listing_id,
    name AS listing_name,
    host_id,
    host_name,
    city,
    neighbourhood,
    room_type,
    price_per_night,
    currency,
    minimum_nights,
    listing_status,
    valid_from,
    valid_to
FROM delta_scan('data/silver/tables/listings')
