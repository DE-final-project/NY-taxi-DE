-- Silver quarantine: rows rejected by the silver_trips cleaning rules, each with the FIRST rule it broke.
{{ config(materialized='table') }}

with unioned as (
    select * from {{ ref('stg_batch_trips') }}
    union all
    select * from {{ ref('stg_stream_trips') }}
),

flagged as (
    select
        *,
        case
            when dropoff_at <= pickup_at                                then 'dropoff_before_pickup'
            when pickup_at < '2025-01-01' or pickup_at >= '2026-01-01'  then 'outside_2025'
            when trip_distance <= 0                                     then 'zero_distance'
            when trip_distance >= 200                                   then 'distance_too_long'
            when fare_amount < 0 or total_amount < 0                    then 'negative_amount'
            when datediff(hour, pickup_at, dropoff_at) > 24             then 'trip_over_24h'
        end as reject_reason
    from unioned
)

select *
from flagged
where reject_reason is not null