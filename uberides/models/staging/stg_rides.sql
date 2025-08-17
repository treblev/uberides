{{ config(materialized='view') }}

with source as (
  select *
  from {{ source('raw_ext','rides_ext') }}
),

renamed as (
  select
    ride_id,
    rider_id,
    driver_id,
    city, state, pickup_zone, dropoff_zone,
    pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,

    -- timestamps
    start_time_utc as pickup_at,
    end_time_utc   as dropoff_at,

    -- metrics
    status,
    distance_miles,
    duration_minutes,
    wait_time_minutes,
    avg_speed_mph,
    traffic_level,
    weather,
    surge_multiplier,

    -- pricing
    base_fare,
    per_mile_rate,
    per_minute_rate,
    tolls,
    taxes,
    coupon_discount,
    fare_total,
    tip,
    platform_fee,
    driver_earnings,

    -- misc
    payment_type,
    device_type,
    rider_rating,
    driver_rating,
    is_weekend,
    is_holiday,
    promo_code,

    -- partition
    dt as ride_date
  from source
)

select * from renamed