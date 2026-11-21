-- Gold: pipeline health per load day and source -> volume, speed and data-quality error rate.
{{ config(materialized='table') }}

with loads as (
    select
        cast(loaded_at as date)  as load_date,
        table_name               as source_table,
        count (*)             as files_loaded,
        sum(rows_loaded)         as rows_loaded,
        sum(seconds)             as load_seconds
    from {{ source('raw', 'load_log') }}
    group by cast(loaded_at as date), table_name
),

-- Every quarantined row remembers its lake file -> link it back to the load that brought it.
rejected as (
    select
        cast(l.loaded_at as date)                                        as load_date,
        l.table_name                                                     as source_table,
        count_big(*)                                                     as rows_rejected,
        sum(case when q.reject_reason = 'duplicate' then 1 else 0 end)   as rows_duplicate
    from {{ ref('quarantine_trips') }} q
    join {{ source('raw', 'load_log') }} l on l.file_path = q._source_file
    group by cast(l.loaded_at as date), l.table_name
)

select
    lo.load_date,
    lo.source_table,
    lo.files_loaded,
    lo.rows_loaded,
    cast(lo.load_seconds as decimal(10, 1))                                   as load_seconds,
    cast(lo.rows_loaded / nullif(lo.load_seconds, 0) as int)               as rows_per_second,
    coalesce(r.rows_rejected, 0)                                              as rows_rejected,
    coalesce(r.rows_duplicate, 0)                                             as rows_duplicate,
    cast(100.0 * coalesce(r.rows_rejected, 0) / nullif(lo.rows_loaded, 0)
         as decimal(5, 2))                                                    as error_rate_pct
from loads lo
left join rejected r
    on r.load_date = lo.load_date and r.source_table = lo.source_table