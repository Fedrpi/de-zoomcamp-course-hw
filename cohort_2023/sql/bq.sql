-- What is count for fhv vehicle records for year 2019

SELECT COUNT(1)
FROM `disco-bedrock-375516.raw.fhv_ny_taxi`;

-- What is the estimated amount of data that will be read 
-- when you execute your query on the External Table and the Materialized Table?

CREATE OR REPLACE TABLE `disco-bedrock-375516.raw.fhv_ny_taxi_nonpatitioned`
as SELECT * FROM `disco-bedrock-375516.raw.fhv_ny_taxi`;

SELECT COUNT(1)
FROM `disco-bedrock-375516.raw.fhv_ny_taxi_nonpatitioned`;

-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

select count(1)
from `disco-bedrock-375516.raw.fhv_ny_taxi_nonpatitioned`
where PUlocationID is null and DOlocationID is null ;

-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 
-- 03/01/2019 and 03/31/2019 (inclusive)

select distinct Affiliated_base_number
from `disco-bedrock-375516.raw.fhv_ny_taxi_nonpatitioned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

CREATE OR REPLACE TABLE `disco-bedrock-375516.raw.fhv_ny_taxi_patitioned`
partition by date(pickup_datetime)
cluster by Affiliated_base_number
as SELECT * FROM `disco-bedrock-375516.raw.fhv_ny_taxi`;

select distinct Affiliated_base_number
from `disco-bedrock-375516.raw.fhv_ny_taxi_patitioned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
