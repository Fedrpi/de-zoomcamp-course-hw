--How many taxi trips were there on January 15

select 
	count(*)
from green_taxi_tripdata
where date(lpep_pickup_datetime) = '2019-01-15';

-- Which was the day with the largest trip distance?

select 
    date(lpep_pickup_datetime)
from 
    green_taxi_tripdata
where trip_distance = (
    select max(trip_distance)
    from green_taxi_tripdata
);

-- In 2019-01-01 how many trips had 2 and 3 passengers?

select 
	passenger_count
   ,count(1)
from green_taxi_tripdata
where date(lpep_pickup_datetime) = '2019-01-01'
  and passenger_count in (2, 3)
group by passenger_count


-- For the passengers picked up in the Astoria Zone 
-- which was the drop up zone that had the largest tip?

select 
    tzdo.zone
from 
    green_taxi_tripdata gtt
left join time_zones tzdo
       on gtt.do_location_id = tzdo.location_id
where tip_amount = (
    select max(tip_amount)
    from green_taxi_tripdata gt
    left join time_zones tzpu
       on gt.pu_location_id = tzpu.location_id
    where tzpu.zone = 'Astoria'
)
limit 1
