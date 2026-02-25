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

    CASE
        WHEN SUM(r.distance_km) > 500 THEN 'High Demand'
        WHEN SUM(r.distance_km) BETWEEN 200 AND 500 THEN 'Medium Demand'
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
