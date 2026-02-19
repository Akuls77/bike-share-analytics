{{ config(
    materialized='view',
    schema='CDS',
    tags=['cds']
) }}

WITH base AS (

    SELECT *
    FROM {{ ref('rds_bike_rides') }}

),

cleaned AS (

    SELECT
        trip_duration,
        start_time,
        stop_time,
        start_station_id,
        start_station_name,
        start_station_latitude,
        start_station_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude,
        end_station_longitude,
        bike_id,
        user_type,
        CAST(birth_year AS INTEGER) AS birth_year,
        {{ map_gender('gender') }} AS gender,
        trip_duration_in_min

    FROM base
    WHERE trip_duration > 0
      AND start_time IS NOT NULL
      AND stop_time IS NOT NULL

)

SELECT * FROM cleaned
