{{ config(materialized='table') }}

with bounds as (
  select
    dateadd(day, -30, min(ride_date)) as start_date,
    dateadd(day,  90, max(ride_date)) as end_date
  from {{ ref('stg_rides') }}
),
span as (
  select
    dateadd(day, g.seq, b.start_date) as d
  from bounds b
  join (
    select seq4() as seq
    from table(generator(rowcount => 2000))
  ) g
  where dateadd(day, g.seq, b.start_date) <= b.end_date
),
final as (
  select
    d as date_day,
    year(d)  as year,
    month(d) as month_num,
    to_char(d, 'Mon') as month_abbrev,
    day(d)   as day_of_month,
    dayofweekiso(d) as dow_iso_num,
    to_char(d, 'Dy') as dow_abbrev,
    weekiso(d) as iso_week,
    quarter(d) as quarter,
    case when dayofweek(d) in (0,6) then true else false end as is_weekend,
    to_date(date_trunc('week', d)) as week_start,
    to_date(date_trunc('month', d)) as month_start,
    to_date(date_trunc('quarter', d)) as quarter_start,
    to_date(date_trunc('year', d)) as year_start
  from span
)

select * from final