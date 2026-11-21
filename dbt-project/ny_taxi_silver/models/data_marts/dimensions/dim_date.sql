{{ config(materialized='table') }}

WITH trip_dates AS (

    SELECT pickup_datetime::DATE AS full_date
    FROM {{ ref('int_taxi_trips_accepted') }}

    UNION

    SELECT dropoff_datetime::DATE AS full_date
    FROM {{ ref('int_taxi_trips_accepted') }}

)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INT AS date_key,
    full_date,

    EXTRACT(YEAR FROM full_date)::INT AS year,
    EXTRACT(QUARTER FROM full_date)::INT AS quarter,
    EXTRACT(MONTH FROM full_date)::INT AS month,
    TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
    EXTRACT(DAY FROM full_date)::INT AS day,

    TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
    EXTRACT(DOW FROM full_date)::INT AS day_of_week,

    EXTRACT(DOW FROM full_date) IN (0, 6) AS is_weekend

FROM trip_dates

WHERE full_date IS NOT NULL