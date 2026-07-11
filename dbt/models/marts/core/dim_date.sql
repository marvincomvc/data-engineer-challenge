-- Date dimension spanning the observed data plus headroom. Generated so
-- consumers get gap-free calendars (weekday, month, quarter) for trend queries.
with bounds as (
    select
        least(
            (select min(cast(created_at as date)) from {{ ref('stg_transactions') }}),
            (select min(reference_date) from {{ ref('stg_reconciliation_runs') }})
        ) as min_date,
        greatest(
            (select max(cast(created_at as date)) from {{ ref('stg_transactions') }}),
            (select max(reference_date) from {{ ref('stg_reconciliation_runs') }})
        ) as max_date
),
spine as (
    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date + 30 from bounds),
        interval '1 day'
    ))::date as date_day
)
select
    date_day,
    extract(year    from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month   from date_day) as month,
    strftime(date_day, '%Y-%m')    as year_month,
    extract(day     from date_day) as day_of_month,
    extract(dow     from date_day) as day_of_week,
    (extract(dow from date_day) in (0, 6)) as is_weekend
from spine
