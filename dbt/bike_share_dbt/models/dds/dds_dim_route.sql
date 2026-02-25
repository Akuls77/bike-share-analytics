{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','dimension']
) }}

WITH routes AS (

    SELECT DISTINCT
        start_station_id,
        end_station_id,
        start_station_latitude,
        start_station_longitude,
        end_station_latitude,
        end_station_longitude
    FROM {{ ref('cds_rides_enriched') }}

),

distance_calc AS (

    SELECT
        start_station_id,
        end_station_id,

        ROUND(
            {{ calculate_haversine_km(
                'start_station_latitude',
                'start_station_longitude',
                'end_station_latitude',
                'end_station_longitude'
            ) }},
            2
        ) AS distance_km

    FROM routes

)

SELECT
    {{ generate_surrogate_key(['start_station_id','end_station_id']) }} AS route_sk,
    start_station_id,
    end_station_id,
    distance_km,

    CASE
        WHEN distance_km < 1 THEN 'Short'
        WHEN distance_km BETWEEN 1 AND 2 THEN 'Medium'
        ELSE 'Long'
    END AS route_category

FROM distance_calc
