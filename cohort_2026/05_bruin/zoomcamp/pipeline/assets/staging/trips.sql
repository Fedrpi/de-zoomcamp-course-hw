/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.payment_lookup
  - ingestion.trips

materialization:
  type: table

columns:
  - name: taxi_type
    type: string
    description: "Type of taxi: yellow or green"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup datetime"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff datetime"
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: "TPEP/LPEP provider ID"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: ratecode_id
    type: integer
    description: "Rate code ID"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"
  - name: pu_location_id
    type: integer
    description: "Pickup TLC Taxi Zone ID"
  - name: do_location_id
    type: integer
    description: "Dropoff TLC Taxi Zone ID"
  - name: payment_type_id
    type: integer
    description: "Payment type code"
  - name: payment_type_name
    type: string
    description: "Payment type name from lookup table"
  - name: fare_amount
    type: float
    description: "Metered fare amount"
    checks:
      - name: non_negative
  - name: extra
    type: float
    description: "Extra charges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge"
  - name: total_amount
    type: float
    description: "Total amount"
    checks:
      - name: non_negative
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge"
  - name: extracted_at
    type: timestamp
    description: "Extraction timestamp"

custom_checks:
  - name: row_count_positive
    description: Ensure table is not empty
    query: SELECT count(*) > 1 FROM staging.trips
    value: 1

@bruin */

WITH filtered AS (
    SELECT
        taxi_type,
        pickup_datetime,
        dropoff_datetime,
        vendor_id,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        trip_distance,
        CAST(ratecode_id AS INTEGER)     AS ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        CAST(payment_type AS INTEGER)    AS payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        extracted_at
    FROM ingestion.trips
    WHERE vendor_id IS NOT NULL
      AND pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime > pickup_datetime
      AND passenger_count  > 0
      AND trip_distance   >= 0
      AND fare_amount     >= 0
      AND total_amount    >= 0
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY taxi_type,
                         vendor_id,
                         pickup_datetime,
                         dropoff_datetime,
                         pu_location_id,
                         do_location_id
            ORDER BY extracted_at DESC
        ) AS _rn
    FROM filtered
)

SELECT
    d.taxi_type,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.vendor_id,
    d.passenger_count,
    d.trip_distance,
    d.ratecode_id,
    d.store_and_fwd_flag,
    d.pu_location_id,
    d.do_location_id,
    d.payment_type_id,
    pl.payment_type_name,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.improvement_surcharge,
    d.total_amount,
    d.congestion_surcharge,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup pl
       ON d.payment_type_id = pl.payment_type_id
WHERE d._rn = 1
