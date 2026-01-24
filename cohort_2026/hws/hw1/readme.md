# HOMEWORK 1

## Question 1

**What's the version of pip in the image in docker with python 3.13**

```bash
docker run --rm python:3.13 pip --version
```

Answer: pip 25.3

## Question 2

**What is the hostname and port that pgadmin should use to connect to the postgres database in docker compose?**

Answer: postgres:5432

## Question 3

**Counting short trips**

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

```sql
select
    count(1)
from ny_taxi.green_trip_data
where (lpep_pickup_datetime >= '2025-11-01'
 and lpep_pickup_datetime <= '2025-12-01')
 and trip_distance <=1;
```

Answer: 8007

## Question 4

**Longest trip for each day**

Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

```sql
select date(lpep_pickup_datetime)
from ny_taxi.green_trip_data
where trip_distance <= 100
order by trip_distance desc
limit 1;
```

Answer: 2025-11-14

## Question 5

**Biggest pickup zone**

Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```sql
select
    t.zone
from ny_taxi.green_trip_data g
left join ny_taxi.taxi_zone_lookup t
       on g.pulocation_id = t.location_id
where date(g.lpep_pickup_datetime) = '2025-11-18'
group by t.zone
order by sum(g.total_amount)  desc
limit 1;
```

Answer: East Harlem North

## Question 6

**Largest tip**

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

```sql
select
    tdo.zone
from ny_taxi.green_trip_data g
left join ny_taxi.taxi_zone_lookup tpu
       on g.pulocation_id = tpu.location_id
left join ny_taxi.taxi_zone_lookup tdo
       on g.dolocation_id = tdo.location_id
where (g.lpep_pickup_datetime >= '2025-11-01'
  and g.lpep_pickup_datetime <= '2025-12-01')
  and tpu.zone = 'East Harlem North'
order by g.tip_amount  desc
limit 1;
```

Answer: Yorkville West

## Question 7

**Terraform Workflow**

Which of the following sequences, respectively, describes the workflow for:

Downloading the provider plugins and setting up backend,
Generating proposed changes and auto-executing the plan
Remove all resources managed by terraform`

Answer: terraform init, terraform apply -auto-approve, terraform destroy
