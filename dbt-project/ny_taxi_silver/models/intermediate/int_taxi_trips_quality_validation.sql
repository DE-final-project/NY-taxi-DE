{{ config(materialized='view') }}

-- Combine batch and streaming taxi trips

WITH all_trips AS (

    SELECT * FROM {{ ref('stg_taxi_batch') }}

    UNION ALL

    SELECT * FROM {{ ref('stg_taxi_stream') }}

),

-- Validate data quality

validated_trips AS (

    SELECT
        *,

        CONCAT_WS(
            '; ',

            CASE
                WHEN vendor_id IS NULL
                THEN 'Missing vendor ID'
            END,

            CASE
                WHEN pickup_datetime IS NULL
                THEN 'Missing pickup datetime'
            END,

            CASE
                WHEN dropoff_datetime IS NULL
                THEN 'Missing dropoff datetime'
            END,

            CASE
                WHEN total_amount IS NULL
                THEN 'Missing total amount'
            END,

            CASE
                WHEN trip_distance IS NULL
                THEN 'Missing trip distance'
            END,

            CASE
                WHEN fare_amount IS NULL
                THEN 'Missing fare amount'
            END,

            CASE
                WHEN passenger_count < 0
                THEN 'Negative passenger count'
            END,

            CASE
                WHEN trip_distance < 0
                THEN 'Negative trip distance'
            END,

            CASE
                WHEN fare_amount < 0
                THEN 'Negative fare amount'
            END,

            CASE
                WHEN total_amount < 0
                THEN 'Negative total amount'
            END,

            CASE
                WHEN pickup_datetime IS NOT NULL
                 AND dropoff_datetime IS NOT NULL
                 AND dropoff_datetime < pickup_datetime
                THEN 'Dropoff before pickup'
            END

        ) AS rejection_reason

    FROM all_trips

)

-- Classify records

SELECT
    *,

    CASE
        WHEN rejection_reason = ''
        THEN 'accepted'
        ELSE 'rejected'
    END AS record_status

FROM validated_trips