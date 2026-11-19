-- Silver: one clean, de-duplicated row per trip from both pipelines (batch + stream).
-- Incremental: each run only processes lake files loaded after the newest one already here.
{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with unioned as (
    select * from {{ ref('stg_batch_trips') }}
    union all
    select * from {{ ref('stg_stream_trips') }}
),

with_load_time as (
    select u.*, l.loaded_at as _loaded_at
    from unioned u
    join {{ source('raw', 'load_log') }} l on l.file_path = u._source_file
    {% if is_incremental() %}
    where l.loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        *,
        {{ trip_id() }} as trip_id,
        datediff(minute, pickup_at, dropoff_at) as trip_duration_min,
        case payment_type_id
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_type
    from with_load_time
    where dropoff_at > pickup_at
      and pickup_at >= '2025-01-01' and pickup_at < '2026-01-01'
      and trip_distance > 0 and trip_distance < 200
      and fare_amount >= 0 and total_amount >= 0
      and datediff(hour, pickup_at, dropoff_at) <= 24
),

deduped as (
    select
        *,
        row_number() over (
            partition by trip_id
            order by case trip_source when 'batch' then 1 else 2 end
        ) as rn
    from cleaned
)

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    trip_duration_min,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    trip_source,
    _source_file,
    _loaded_at
from deduped
where rn = 1