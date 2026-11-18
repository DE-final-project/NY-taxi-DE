{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_type_key,
    payment_type

FROM (
    SELECT DISTINCT payment_type
    FROM {{ ref('int_taxi_trips_accepted') }}
    WHERE payment_type IS NOT NULL
) p_t