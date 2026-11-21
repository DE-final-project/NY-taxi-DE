-- Gold: one row per NYC taxi zone. Used twice by fact_trips (pickup zone and dropoff zone).
{{ config(materialized='table') }}

select
    LocationID      as location_id,
    Borough         as borough,
    Zone            as zone_name,
    service_zone
from {{ ref('taxi_zone_lookup') }}