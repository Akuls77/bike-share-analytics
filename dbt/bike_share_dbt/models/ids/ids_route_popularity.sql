{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    r.start_station_id,
    r.end_station_id,
    r.route_category,
    r.distance_km,
    COUNT(*) AS total_rides,
    SUM(r.distance_km) AS total_distance_km,
    (COUNT(*) / NULLIF(r.distance_km,0)) AS rides_per_km,

    CASE
        WHEN COUNT(*) > 500 THEN 'High Demand'
        WHEN COUNT(*) BETWEEN 250 AND 500 THEN 'Medium Demand'
        ELSE 'Low Demand'
    END AS demand_category

FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_route') }} r
    ON f.route_sk = r.route_sk

GROUP BY
    r.start_station_id,
    r.end_station_id,
    r.route_category,
    r.distance_km
