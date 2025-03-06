WITH
source AS (
    SELECT *
    FROM {{ source('nyc_yellowtaxi', 'raw_historical_weather') }}
),

flatten AS (
    SELECT
        f.value:time::TIMESTAMP AS observed_at,
        f.value:temp::DECIMAL(5, 1) AS air_temperature_celsius,
        f.value:dwpt::DECIMAL(5, 1) AS dew_point_celcius,
        f.value:rhum::INTEGER AS relative_humidity_percent,
        f.value:prcp::DECIMAL(5, 1) AS hourly_precipitation_mm,
        f.value:snow::INTEGER AS snow_depth_mm,
        f.value:wdir::INTEGER AS wind_direction_degrees,
        f.value:wspd::DECIMAL(5, 1) AS wind_speed_kmh,
        f.value:wpgt::FLOAT AS peak_wind_gust_kmh,
        f.value:pres::DECIMAL(8, 1) AS air_pressure_hpa,
        f.value:tsun::INTEGER AS hourly_sunshine_min,
        f.value:coco::INTEGER AS weather_condition_code
    FROM source,
        LATERAL FLATTEN(input => data:data) AS f
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['observed_at']) }} AS weather_id,
    observed_at,
    air_temperature_celsius,
    dew_point_celcius,
    relative_humidity_percent,
    hourly_precipitation_mm,
    snow_depth_mm,
    wind_direction_degrees,
    wind_speed_kmh,
    peak_wind_gust_kmh,
    air_pressure_hpa,
    hourly_sunshine_min,
    weather_condition_code
FROM flatten
