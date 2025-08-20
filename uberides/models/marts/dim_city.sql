{{ config(materialized='table') }}

with cities(city_id, city_name, city_state, start_date, end_date) as (
  select column1::int, column2::string, column3::string, column4::date, column5::date
  from values
    (1, 'Phoenix',      'AZ', '2000-01-01'::date, null),
    (2, 'Los Angeles',  'CA', '2000-01-01'::date, null),
    (3, 'San Francisco','CA', '2000-01-01'::date, null),
    (4, 'New York',     'NY', '2000-01-01'::date, null),
    (5, 'Chicago',      'IL', '2000-01-01'::date, null)
)

select * from cities