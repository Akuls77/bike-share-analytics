{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    ride_date,
    season,
    is_weekend,
    COUNT(*) AS total_rides,
    AVG(trip_duration_in_min) AS avg_trip_duration_min
FROM {{ ref('dds_fact_rides') }}
GROUP BY
    ride_date,
    season,
    is_weekend
