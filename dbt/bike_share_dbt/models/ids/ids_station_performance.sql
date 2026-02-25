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

    CASE
        WHEN total_departures > 3500 AND total_arrivals > 3500
            THEN 'High Departure / High Arrival'

        WHEN total_departures > 3500 AND total_arrivals <= 3500
            THEN 'High Departure / Low Arrival'

        WHEN total_departures <= 3500 AND total_arrivals > 3500
            THEN 'Low Departure / High Arrival'

        ELSE 'Low Departure / Low Arrival'
    END AS activity_category

FROM departures d
LEFT JOIN arrivals a
    ON d.station_name = a.station_name
