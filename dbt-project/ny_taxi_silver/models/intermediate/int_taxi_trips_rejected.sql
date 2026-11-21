{{ config(materialized='table') }}

SELECT *
FROM {{ ref('int_taxi_trips_quality_validation') }}

WHERE record_status = 'rejected'