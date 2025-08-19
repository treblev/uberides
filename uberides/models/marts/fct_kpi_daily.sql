{{ config(materialized='table') }}

with cal as (
    select date_day from {{ ref('dim_date') }}
),
r as (
    select ride_date, status, fare_total, duration_minutes from {{ ref('stg_rides') }}
),
summary as (
      select
    cal.date_day as ride_date,
    count_if(r.ride_date is not null) as rides_total,
    count_if(r.status = 'completed') as rides_completed,
    count_if(r.status in ('cancelled','no_show')) as rides_not_completed,
    coalesce(sum(r.fare_total), 0) as revenue_total,
    avg(nullif(r.fare_total, 0))  as avg_fare,
    avg(r.duration_minutes) as avg_duration_min,
    count_if(r.status = 'cancelled') * 1.0 / nullif(count(*),0) as cancel_rate,
    count_if(r.status = 'no_show') * 1.0 / nullif(count(*),0)   as no_show_rate
    from cal 
    left join r on r.ride_date = cal.date_day 
    group by cal.date_day 
)
select * from summary