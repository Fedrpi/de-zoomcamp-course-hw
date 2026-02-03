# HOMEWORK 2

## Question 1

*Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?*

Answer: 134.5 MiB

## Question 2

*What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?*

Answer: green_tripdata_2020-04.csv

## Question 3

*How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?*

```sql
select count(1) from public.yellow_tripdata
where date_part('year', tpep_pickup_datetime) = 2020;
```

Answer: 24648499

## Question 4

*How many rows are there for the Green Taxi data for all CSV files in the year 2020?*

```sql
select count(1) from public.green_tripdata
where date_part('year', lpep_pickup_datetime) = 2020;
```

Answer: 1734039

## Question 5

*How many rows are there for the Yellow Taxi data for the March 2021 CSV file?*

```sql
select count(1) from public.yellow_tripdata
where date_part('year', tpep_pickup_datetime) = 2021 
  and date_part('month', tpep_pickup_datetime) = 3;
```

Answer: 1925130

## Question 6

*How would you configure the timezone to New York in a Schedule trigger?*

Answer: America/New_York
