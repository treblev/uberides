{{ config(
    materialized='incremental',
    unique_key=['city', 'ride_date']
) }}

with daily as (
  select
    city,
    cast(ride_date as date) as ride_date,
    coalesce(sum(rides_total),0)            as rides_total,
    coalesce(sum(revenue_total),0)       as revenue_total,
    coalesce(avg(avg_fare),0)        as avg_fare,          
    coalesce(avg(avg_duration_min),0) as avg_duration_min,  
    coalesce(avg(avg_wait_min),0) as avg_wait_min,      
    coalesce(avg(rides_not_completed),0)      as cancel_rate,
    coalesce(avg(no_show_rate),0)     as no_show_rate
  from {{ ref('fct_rides_daily_city') }}
  {% if is_incremental() %}
    where ride_date > (select coalesce(max(ride_date)-30,to_date('2024-08-17')) from {{ this }} )
  {% endif %}
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
