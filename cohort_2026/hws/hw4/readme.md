# HOMEWORK 4

## Question 1

*If you run dbt run --select int_trips_unioned, what models will be built?*

Answer: int_trips_unioned only

## Question 2

*Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.*

Answer: dbt will fail the test, returning a non-zero exit code

## Question 3

*What is the count of records in the fct_monthly_zone_revenue model?*

```sql
select count(1) from prod.fct_monthly_zone_revenue;
```

Answer: 12184

## Question 4

*Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.*

```sql
select
    pickup_zone,
    sum(revenue_monthly_total_amount)
from prod.fct_monthly_zone_revenue
where service_type = 'Green'
  and date_part('year', revenue_month) = '2020'
group by pickup_zone
order by sum(revenue_monthly_total_amount) desc
limit 1;
```

Answer: East Harlem North

## Question 5

*Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?*

```sql
select
    sum(total_monthly_trips)
from prod.fct_monthly_zone_revenue
where service_type = 'Green'
  and date_part('year', revenue_month) = '2019'
  and date_part('month', revenue_month) = '10';
```

Answer: 384624

## Question 6

*What is the count of records in stg_fhv_tripdata?*

```sql
select
    count(1)
from prod.stg_fhv_tripdata;
```

Answer: 43244693
