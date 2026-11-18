-- Gold fact: one row per trip (grain = trip). Keys point to the dims; measures are numbers to aggregate.
{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

select
    trip_id,

    -- keys to the dimensions
    year(pickup_at) * 10000 + month(pickup_at) * 100 + day(pickup_at) as pickup_date_key,
    datepart(hour, pickup_at) as pickup_hour,
    vendor_id,
    payment_type_id,
    pickup_location_id,
    dropoff_location_id,

    -- measures
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,

    -- lineage
    trip_source,
    _loaded_at
from {{ ref('silver_trips') }}
{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}