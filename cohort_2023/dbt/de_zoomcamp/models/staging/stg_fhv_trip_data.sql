{{
  config(
    materialized = 'view',
    )
}}
select 
    cast(pu_location_id as integer) as  pickup_location_id,
    cast(do_location_id as integer) as dropoff_location_id,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    dispatching_base_num,
    affiliated_base_number
from {{ source('staging', 'fhv') }}
{% if var('is_test_run', default=true) %}
limit 100  
{% endif %}