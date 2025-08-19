{{ config(materialized='table') }}

WITH cities as (
    select 1 as city_id, 'Phoenix' as city_name, 'AZ' as city_state, '2000-01-01' as start_date, NULL as end_date
    UNION 
    select 2 as city_id, 'Los Angeles' as city_name, 'CA' as city_state, '2000-01-01' as start_date, NULL as end_date
    UNION 
    select 3 as city_id, 'San Francisco' as city_name, 'CA' as city_state, '2000-01-01' as start_date, NULL as end_date
    UNION 
    select 4 as city_id, 'New York' as city_name, 'NY' as city_state, '2000-01-01' as start_date, NULL as end_date
    UNION 
    select 5 as city_id, 'Chicago' as city_name, 'IL' as city_state, '2000-01-01' as start_date, NULL as end_date
)
SELECT * FROM cities  