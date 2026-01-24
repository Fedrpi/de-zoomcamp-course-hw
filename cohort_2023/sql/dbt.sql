-- What is the count of records in the model fact_trips 
-- after running all models with the test run variable 
-- disabled and filtering for 2019 and 2020 data only (pickup datetime)

select count(1)  from `dbt.fact_trips`
where date(pickup_datetime) < '2021-01-01'


-- What is the count of records in the model stg_fhv_tripdata 
-- after running all models with the test run variable disabled (:false)

select count(1)  from `dbt.stg_fhv_trip_data`
where date(pickup_datetime) < '2020-01-01'

-- What is the count of records in the model fact_fhv_trips 
-- after running all dependencies with the test run variable disabled (:false)

select count(1)  from `dbt.fact_fhv_trips`
where date(pickup_datetime) < '2020-01-01'
