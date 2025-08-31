{{ config(materialized='view') }}

with daily as (
  select
    city,
    ride_date,
    rides_total,
    rides_completed,
    rides_not_completed,
    revenue_total,
    avg_fare,
    avg_duration_min,
    avg_wait_min,
    cancel_rate,
    no_show_rate
  from {{ ref('fct_rides_daily_city') }}
),

rolling as (
  select
    city,
    ride_date,
    sum(rides_total)        over (partition by city order by ride_date rows between 29 preceding and current row) as rides_30d,
    sum(revenue_total)      over (partition by city order by ride_date rows between 29 preceding and current row) as revenue_30d,
    avg(avg_fare)           over (partition by city order by ride_date rows between 29 preceding and current row) as avg_fare_30d,
    avg(avg_duration_min)   over (partition by city order by ride_date rows between 29 preceding and current row) as avg_dur_30d,
    avg(avg_wait_min)       over (partition by city order by ride_date rows between 29 preceding and current row) as avg_wait_30d,
    avg(cancel_rate)        over (partition by city order by ride_date rows between 29 preceding and current row) as cancel_rate_30d,
    avg(no_show_rate)       over (partition by city order by ride_date rows between 29 preceding and current row) as no_show_rate_30d
    sum(rides_total)        over (partition by city order by ride_date rows between 6 preceding and current row) as rides_7d,
    sum(revenue_total)      over (partition by city order by ride_date rows between 6 preceding and current row) as revenue_7d,
    avg(avg_fare)           over (partition by city order by ride_date rows between 6 preceding and current row) as avg_fare_7d,
    avg(avg_duration_min)   over (partition by city order by ride_date rows between 6 preceding and current row) as avg_dur_7d,
    avg(avg_wait_min)       over (partition by city order by ride_date rows between 6 preceding and current row) as avg_wait_7d,
    avg(cancel_rate)        over (partition by city order by ride_date rows between 6 preceding and current row) as cancel_rate_7d,
    avg(no_show_rate)       over (partition by city order by ride_date rows between 6 preceding and current row) as no_show_rate_7d
  from daily
)
select * from rolling 