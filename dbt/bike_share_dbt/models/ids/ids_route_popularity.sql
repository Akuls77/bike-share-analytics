{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    r.start_station_id,
    r.end_station_id,
    COUNT(*) AS total_rides,
    AVG(f.trip_duration_in_min) AS avg_duration_min
FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_route') }} r
    ON f.route_sk = r.route_sk
GROUP BY
    r.start_station_id,
    r.end_station_id
