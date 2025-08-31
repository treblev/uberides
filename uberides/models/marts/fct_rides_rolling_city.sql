{{ config(materialized='incremental',
          unique_key='city||to_char(ride_date, ''YYYY-MM-DD'')') }}

with daily as (
  select
    city,
    ride_date,
    sum(rides)            as rides_total,
    sum(gross_fare)       as revenue_total,
    avg(avg_fare)         as avg_fare,          -- adjust to your column names
    avg(avg_duration_min) as avg_duration_min,  -- remove if not in source
    avg(avg_wait_min)     as avg_wait_min,      -- remove if not in source
    avg(cancel_rate)      as cancel_rate,
    avg(no_show_rate)     as no_show_rate
  from {{ ref('fct_rides_daily_city') }}
  group by 1,2
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
    avg(no_show_rate)       over (partition by city order by ride_date rows between 29 preceding and current row) as no_show_rate_30d,
    sum(rides_total)        over (partition by city order by ride_date rows between 6 preceding and current row)  as rides_7d,
    sum(revenue_total)      over (partition by city order by ride_date rows between 6 preceding and current row)  as revenue_7d,
    avg(avg_fare)           over (partition by city order by ride_date rows between 6 preceding and current row)  as avg_fare_7d,
    avg(avg_duration_min)   over (partition by city order by ride_date rows between 6 preceding and current row)  as avg_dur_7d,
    avg(avg_wait_min)       over (partition by city order by ride_date rows between 6 preceding and current row)  as avg_wait_7d,
    avg(cancel_rate)        over (partition by city order by ride_date rows between 6 preceding and current row)  as cancel_rate_7d,
    avg(no_show_rate)       over (partition by city order by ride_date rows between 6 preceding and current row)  as no_show_rate_7d
  from daily
)
select * from rolling