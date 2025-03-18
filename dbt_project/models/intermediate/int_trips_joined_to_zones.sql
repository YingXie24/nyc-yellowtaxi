WITH trips AS (SELECT * FROM {{ ref("stg_tlc__trips") }}),

zones AS (SELECT * FROM {{ ref("stg_tlc__zones") }})

SELECT
    t.*,
    z1.borough AS pickup_borough,
    z1.zone AS pickup_zone,
    z1.service_zone AS pickup_service_zone,
    z2.borough AS dropoff_borough,
    z2.zone AS dropoff_zone,
    z2.service_zone AS dropoff_service_zone
FROM trips AS t
INNER JOIN zones AS z1
    ON t.pickup_location_id = z1.location_id
INNER JOIN zones AS z2
    ON t.dropoff_location_id = z2.location_id
