{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

WITH departures AS (

    SELECT
        s.station_name,
        COUNT(*) AS total_departures,
        SUM(f.trip_duration_in_min) AS total_duration_min,
        SUM(r.distance_km) AS total_distance_km
    FROM {{ ref('dds_fact_rides') }} f
    JOIN {{ ref('dds_dim_station') }} s
        ON f.start_station_sk = s.station_sk
    JOIN {{ ref('dds_dim_route') }} r
        ON f.route_sk = r.route_sk
    GROUP BY s.station_name
),

arrivals AS (

    SELECT
        s.station_name,
        COUNT(*) AS total_arrivals
    FROM {{ ref('dds_fact_rides') }} f
    JOIN {{ ref('dds_dim_station') }} s
        ON f.end_station_sk = s.station_sk
    GROUP BY s.station_name
)

SELECT
    d.station_name,
    d.total_departures,
    a.total_arrivals,
    (a.total_arrivals - d.total_departures) AS net_flow,

    CASE
        WHEN d.total_departures > 5000 THEN 'High Activity'
        WHEN d.total_departures BETWEEN 1000 AND 5000 THEN 'Medium Activity'
        ELSE 'Low Activity'
    END AS activity_category

FROM departures d
LEFT JOIN arrivals a
    ON d.station_name = a.station_name
