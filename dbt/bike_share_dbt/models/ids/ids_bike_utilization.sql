{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

WITH bike_stats AS (

    SELECT
        b.bike_id,
        COUNT(*) AS total_rides,
        SUM(f.trip_duration_in_min) AS total_trip_duration_min,
        ROUND(AVG(f.trip_duration_in_min),2) AS avg_trip_duration_min,
        SUM(r.distance_km) AS total_distance_km

    FROM {{ ref('dds_fact_rides') }} f
    JOIN {{ ref('dds_dim_bike') }} b
        ON f.bike_sk = b.bike_sk
    JOIN {{ ref('dds_dim_route') }} r
        ON f.route_sk = r.route_sk

    GROUP BY b.bike_id
)

SELECT
    *,
    (total_rides * avg_trip_duration_min) AS utilization_score,

    CASE
        WHEN (total_rides * avg_trip_duration_min) > 10000 THEN 'High'
        WHEN (total_rides * avg_trip_duration_min) BETWEEN 5000 AND 10000 THEN 'Medium'
        ELSE 'Low'
    END AS usage_category

FROM bike_stats