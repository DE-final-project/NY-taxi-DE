-- Reconciliation: every raw row must end up in silver_trips OR quarantine_trips.
-- Only exact duplicates (removed by the dedup) may vanish, so we allow a 0.1% tolerance.
-- Singular test: FAILS if this query returns any rows.
with counts as (
    select
        (select count_big(*) from {{ source('raw', 'batch_trips') }})
      + (select count_big(*) from {{ source('raw', 'stream_trips') }})   as raw_rows,
        (select count_big(*) from {{ ref('silver_trips') }})              as silver_rows,
        (select count_big(*) from {{ ref('quarantine_trips') }})       as quarantine_rows
),

checked as (
    select *, raw_rows - silver_rows - quarantine_rows as unexplained_rows
    from counts
)

select *
from checked
where unexplained_rows < 0                       -- a row landed in BOTH tables
   or unexplained_rows > raw_rows * 0.001    -- more rows lost than duplicates can explain