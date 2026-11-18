{{ config(materialized='table') }}

SELECT

    -- Date Foreign Keys
    pickup_date.date_key AS pickup_date_key,
    dropoff_date.date_key AS dropoff_date_key,

    -- Dimension Foreign Keys
    vendor.vendor_key,
    payment.payment_type_key,
    pickup_location.location_key AS pickup_location_key,
    dropoff_location.location_key AS dropoff_location_key,

    -- Trip Information
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.passenger_count,
    trips.trip_distance,
    trips.ratecode_id,

    -- measures
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.cbd_congestion_fee,

    -- Source Information
    trips.source_type,
    trips.source_month

FROM {{ ref('int_taxi_trips_accepted') }} AS trips

LEFT JOIN {{ ref('dim_vendor') }} AS vendor
    ON trips.vendor_id = vendor.vendor_id

LEFT JOIN {{ ref('dim_payment_type') }} AS payment
    ON trips.payment_type = payment.payment_type

LEFT JOIN {{ ref('dim_location') }} AS pickup_location
    ON trips.pu_location_id = pickup_location.location_id

LEFT JOIN {{ ref('dim_location') }} AS dropoff_location
    ON trips.do_location_id = dropoff_location.location_id

LEFT JOIN {{ ref('dim_date') }} AS pickup_date
    ON trips.pickup_datetime::DATE = pickup_date.full_date

LEFT JOIN {{ ref('dim_date') }} AS dropoff_date
    ON trips.dropoff_datetime::DATE = dropoff_date.full_date