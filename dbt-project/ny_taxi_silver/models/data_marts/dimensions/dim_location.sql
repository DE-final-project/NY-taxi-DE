{{ config(materialized='table') }}

WITH all_locations AS (

    SELECT pu_location_id AS location_id
    FROM {{ ref('int_taxi_trips_accepted') }}
    WHERE pu_location_id IS NOT NULL

    UNION

    SELECT do_location_id AS location_id
    FROM {{ ref('int_taxi_trips_accepted') }}
    WHERE do_location_id IS NOT NULL

)

SELECT
    ROW_NUMBER() OVER (ORDER BY location_id) AS location_key,
    location_id

FROM all_locations