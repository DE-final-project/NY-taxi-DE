-- Silver quarantine: rows rejected by the cleaning rules (with the FIRST rule broken)
-- OR extra duplicate copies of a trip silver_trips already kept. raw = silver + quarantine, exactly.
{{ config(materialized='table') }}

with unioned as (
    select * from {{ ref('stg_batch_trips') }}
    union all
    select * from {{ ref('stg_stream_trips') }}
),

flagged as (
    select
        *,
        {{ trip_id() }} as trip_id,
        case
            when dropoff_at <= pickup_at                                then 'dropoff_before_pickup'
            when pickup_at < '2025-01-01' or pickup_at >= '2026-01-01'  then 'outside_2025'
            when trip_distance <= 0                                     then 'zero_distance'
            when trip_distance >= 200                                   then 'distance_too_long'
            when fare_amount < 0 or total_amount < 0                    then 'negative_amount'
            when datediff(hour, pickup_at, dropoff_at) > 24             then 'trip_over_24h'
        end as rule_broken
    from unioned
),

-- Same ordering as silver_trips: the batch copy wins, extra copies are duplicates.
ranked as (
    select
        *,
        row_number() over (
            partition by trip_id, rule_broken
            order by case trip_source when 'batch' then 1 else 2 end
        ) as copy_number
    from flagged
)

select
    *,
    coalesce(rule_broken, 'duplicate') as reject_reason
from ranked
where rule_broken is not null   -- broke a cleaning rule
   or copy_number > 1           -- valid, but an extra copy of a trip silver already kept