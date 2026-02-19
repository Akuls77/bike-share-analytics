{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    s.station_name,
    COUNT(*) AS total_starts,
    AVG(f.trip_duration_in_min) AS avg_duration_min
FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_station') }} s
    ON f.start_station_sk = s.station_sk
GROUP BY s.station_name
