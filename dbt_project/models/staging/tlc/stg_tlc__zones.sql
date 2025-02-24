WITH source AS (
    SELECT *
    FROM {{ ref('seed_tlc_taxi_zone') }}
),
renamed AS (
    SELECT locationid AS location_id,
        borough,
        zone,
        service_zone
    FROM source
)
SELECT *
FROM renamed