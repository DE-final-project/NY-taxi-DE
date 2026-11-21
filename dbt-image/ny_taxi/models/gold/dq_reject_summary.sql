-- Gold: quarantined rows per load day, source and reason, with the rule's explanation (for the Operations dashboard).
{{ config(materialized='table') }}

with rejected as (
    select
        cast(l.loaded_at as date)  as load_date,
        l.table_name               as source_table,
        q.reject_reason,
        count_big(*)               as rows_rejected
    from {{ ref('quarantine_trips') }} q
    join {{ source('raw', 'load_log') }} l on l.file_path = q._source_file
    group by cast(l.loaded_at as date), l.table_name, q.reject_reason
)

select
    r.load_date,
    r.source_table,
    r.reject_reason,
    r.rows_rejected,
    d.[rule],
    d.root_cause,
    d.action,
    d.status
from rejected r
left join {{ ref('dq_rules') }} d on d.reject_reason = r.reject_reason
