WITH source AS (
    SELECT *
    FROM NYC_YELLOWTAXI.RAW.TAXI_TRIPS
),
renamed AS (
    SELECT --ids
        { { dbt_utils.generate_surrogate_key(
            ['vendorid', 'pulocationid', 'dolocationid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
        ) } } AS trip_id,
        vendorid AS vendor_id,
        pulocationid AS pickup_location_id,
        dolocationid AS dropoff_location_id,
        ratecodeid AS rate_code_id,
        --strings
        tpep_pickup_datetime AS pickup_unixtime,
        tpep_dropoff_datetime AS dropoff_unixtime,
        CASE
            WHEN payment_type = 1 THEN 'credit card'
            WHEN payment_type = 2 THEN 'cash'
            WHEN payment_type = 3 THEN 'no charge'
            WHEN payment_type = 4 THEN 'dispute'
            WHEN payment_type = 5 THEN 'unknown'
            WHEN payment_type = 6 THEN 'voided trip'
            WHEN payment_type IS NULL THEN NULL
            ELSE 'error'
        END AS payment_type,
        --numerics
        passenger_count,
        trip_distance,
        fare_amount,
        extra AS extra_surcharge,
        mta_tax,
        improvement_surcharge,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,
        airport_fee,
        --booleans
        CASE
            WHEN store_and_fwd_flag = 'Y' THEN TRUE
            WHEN store_and_fwd_flag = 'N' THEN False
            ELSE NULL
        END AS is_store_and_forward --dates
        --timestamps
    FROM source
),
typed AS (
    SELECT --ids
        trip_id,
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        rate_code_id,
        --strings
        payment_type,
        pickup_unixtime,
        dropoff_unixtime,
        --numerics
        passenger_count,
        trip_distance,
        fare_amount::DECIMAL(10, 2) AS fare_amount,
        extra_surcharge::DECIMAL(10, 2) AS extra_surcharge,
        mta_tax::DECIMAL(10, 2) AS mta_tax,
        improvement_surcharge::DECIMAL(10, 2) AS improvement_surcharge,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge::DECIMAL(10, 2) AS congestion_surcharge,
        airport_fee,
        --booleans
        is_store_and_forward,
        --dates
        --timestamps
        to_timestamp_ntz(pickup_unixtime) AS pickup_at,
        to_timestamp_ntz(dropoff_unixtime) AS dropoff_at
    FROM renamed
)
SELECT *
FROM typed