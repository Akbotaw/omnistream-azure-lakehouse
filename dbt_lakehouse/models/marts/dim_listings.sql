{{ config(materialized='table') }}

SELECT
    *
FROM {{ ref('stg_listings') }}
WHERE valid_to IS NULL