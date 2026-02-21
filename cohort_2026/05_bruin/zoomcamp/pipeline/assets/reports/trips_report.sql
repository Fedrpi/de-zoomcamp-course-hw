/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: "Date of trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type name"
    primary_key: true
  - name: trip_count
    type: integer
    description: "Number of trips"
    checks:
      - name: positive
  - name: total_passengers
    type: integer
    description: "Total number of passengers"
  - name: total_distance_miles
    type: float
    description: "Total trip distance in miles"
  - name: avg_distance_miles
    type: float
    description: "Average trip distance in miles"
  - name: total_fare_amount
    type: float
    description: "Sum of metered fare amounts"
  - name: total_tip_amount
    type: float
    description: "Sum of tip amounts"
  - name: total_tolls_amount
    type: float
    description: "Sum of tolls amounts"
  - name: total_amount
    type: float
    description: "Sum of total amounts charged"
  - name: avg_total_amount
    type: float
    description: "Average total amount per trip"

custom_checks:
  - name: row_count_positive
    description: Ensure report is not empty
    query: SELECT count(*) > 1 FROM reports.trips_report
    value: 1

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)          AS trip_date,
    taxi_type,
    COALESCE(payment_type_name, 'Unknown') AS payment_type_name,
    COUNT(*)                               AS trip_count,
    SUM(passenger_count)                   AS total_passengers,
    ROUND(SUM(trip_distance), 2)           AS total_distance_miles,
    ROUND(AVG(trip_distance), 2)           AS avg_distance_miles,
    ROUND(SUM(fare_amount), 2)             AS total_fare_amount,
    ROUND(SUM(tip_amount), 2)              AS total_tip_amount,
    ROUND(SUM(tolls_amount), 2)            AS total_tolls_amount,
    ROUND(SUM(total_amount), 2)            AS total_amount,
    ROUND(AVG(total_amount), 2)            AS avg_total_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime <  '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'Unknown')
ORDER BY
    trip_date,
    taxi_type,
    payment_type_name
