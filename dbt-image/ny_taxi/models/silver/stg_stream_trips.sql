-- Staging: stream trips with clean column names. No filtering here (silver_trips does that).
{{ config(materialized='view') }}

select
    VendorID               as vendor_id,
    tpep_pickup_datetime   as pickup_at,
    tpep_dropoff_datetime  as dropoff_at,
    passenger_count,
    trip_distance,
    RatecodeID             as rate_code_id,
    store_and_fwd_flag,
    PULocationID           as pickup_location_id,
    DOLocationID           as dropoff_location_id,
    payment_type           as payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee            as airport_fee,
    cbd_congestion_fee,
    'stream'            as trip_source,
    _source_file
from {{ source('raw', 'stream_trips') }}