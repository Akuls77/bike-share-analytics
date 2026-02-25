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
    SUM(trip_duration_in_min) AS total_trip_duration_min,
    SUM(r.distance_km) AS total_distance_km,
    ROUND(AVG(trip_duration_in_min),2) AS avg_trip_duration_min

FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_route') }} r
    ON f.route_sk = r.route_sk

GROUP BY
    ride_date,
    season,
    is_weekend
