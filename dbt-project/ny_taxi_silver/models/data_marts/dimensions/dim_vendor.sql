{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY vendor_id) AS vendor_key,
    vendor_id

FROM (
    SELECT DISTINCT vendor_id
    FROM {{ ref('int_taxi_trips_accepted') }}
    WHERE vendor_id IS NOT NULL
) v