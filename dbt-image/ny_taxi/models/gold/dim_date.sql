-- Gold: one row per calendar day of 2025, with the attributes business questions group by.
{{ config(materialized='table') }}

with days as (
    select dateadd(day, value, cast('2025-01-01' as date)) as full_date
    from generate_series(0, 364)
)

select
    year(full_date) * 10000 + month(full_date) * 100 + day(full_date) as date_key,
    full_date,
    year(full_date)                 as year,
    datepart(quarter, full_date)    as quarter,
    month(full_date)                as month,
    datename(month, full_date)      as month_name,
    day(full_date)                  as day_of_month,
    datename(weekday, full_date)    as day_name,
    case when datename(weekday, full_date) in ('Saturday', 'Sunday') then 1 else 0 end as is_weekend
from days