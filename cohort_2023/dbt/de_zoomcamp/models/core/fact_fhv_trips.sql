{{
  config(
    materialized = 'table',
    )
}}

with fhv_trip as (
    select 
        *
    from {{ ref('stg_fhv_trip_data') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    ft.pickup_location_id,
    pdz.borough as pickup_borough, 
    pdz.zone as pickup_zone, 
    ft.dropoff_location_id,
    ddz.borough as dropoff_borough, 
    ddz.zone as dropoff_zone, 
    ft.pickup_datetime,
    ft.dropoff_datetime,
    ft.dispatching_base_num,
    ft.affiliated_base_number
from fhv_trip ft
inner join dim_zones pdz
        on ft.pickup_location_id = pdz.location_id
inner join dim_zones ddz
        on ft.pickup_location_id = ddz.location_id
