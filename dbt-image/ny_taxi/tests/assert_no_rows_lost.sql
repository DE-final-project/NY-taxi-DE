-- Reconciliation: every raw row must end up in silver_trips OR quarantine_trips, exactly once.
-- Duplicates are quarantined too (reject_reason = 'duplicate'), so no tolerance is needed.
-- Singular test: FAILS if this query returns any rows.
with counts as (
    select
        (select count_big(*) from {{ source('raw', 'batch_trips') }})
      + (select count_big(*) from {{ source('raw', 'stream_trips') }})   as raw_rows,
        (select count_big(*) from {{ ref('silver_trips') }})              as silver_rows,
        (select count_big(*) from {{ ref('quarantine_trips') }})          as quarantine_rows
),

checked as (
    select *, raw_rows - silver_rows - quarantine_rows as unexplained_rows
    from counts
)

select *
from checked
where unexplained_rows <> 0