{{ config(
    materialized='incremental',
    unique_key='city||ride_date',
    incremental_strategy='delete+insert'
) }}

with src as (
  select
    ride_date,
    city,
    status,
    fare_total,
    duration_minutes,
    wait_time_minutes,
    surge_multiplier
  from {{ ref('stg_rides') }}

  {% if is_incremental() %}
    where ride_date > (select coalesce(max(ride_date), '1900-01-01') from {{ this }})
  {% endif %}
),

agg as (
  select
    city,
    ride_date,

    count(*)                                      as rides_total,
    count_if(status = 'completed')                as rides_completed,
    count_if(status in ('cancelled','no_show'))   as rides_not_completed,

    sum(fare_total)                               as revenue_total,
    avg(nullif(fare_total,0))                     as avg_fare,
    avg(duration_minutes)                         as avg_duration_min,
    avg(wait_time_minutes)                        as avg_wait_min,

    avg(surge_multiplier)                         as avg_surge,
    count_if(surge_multiplier > 1) * 1.0 / nullif(count(*),0) as pct_surge,

    count_if(status = 'cancelled') * 1.0 / nullif(count(*),0) as cancel_rate,
    count_if(status = 'no_show') * 1.0 / nullif(count(*),0)   as no_show_rate
  from src
  group by city, ride_date
)
select * from agg