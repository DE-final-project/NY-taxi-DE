{{ config(materialized='view') }}

SELECT
    "VendorID"::BIGINT AS vendor_id,
    tpep_pickup_datetime::TIMESTAMP AS pickup_datetime,
    tpep_dropoff_datetime::TIMESTAMP AS dropoff_datetime,
    passenger_count::DOUBLE PRECISION AS passenger_count,
    trip_distance::DOUBLE PRECISION AS trip_distance,
    "RatecodeID"::BIGINT AS ratecode_id,
    store_and_fwd_flag::TEXT AS store_and_fwd_flag,
    "PULocationID"::BIGINT AS pu_location_id,
    "DOLocationID"::BIGINT AS do_location_id,
    payment_type::BIGINT AS payment_type,
    fare_amount::DOUBLE PRECISION AS fare_amount,
    extra::DOUBLE PRECISION AS extra,
    mta_tax::DOUBLE PRECISION AS mta_tax,
    tip_amount::DOUBLE PRECISION AS tip_amount,
    tolls_amount::DOUBLE PRECISION AS tolls_amount,
    improvement_surcharge::DOUBLE PRECISION AS improvement_surcharge,
    total_amount::DOUBLE PRECISION AS total_amount,
    congestion_surcharge::DOUBLE PRECISION AS congestion_surcharge,
    "Airport_fee"::DOUBLE PRECISION AS airport_fee,
    cbd_congestion_fee::DOUBLE PRECISION AS cbd_congestion_fee,
    source_month::TEXT AS source_month,
    'batch'::TEXT AS source_type

FROM {{ source('bronze', 'batch_trips') }}