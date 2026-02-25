{{ config(
    materialized='view',
    schema='CDS',
    tags=['cds']
) }}

WITH base AS (

    SELECT *
    FROM {{ source('rds', 'raw_bike_rides') }}

),

cleaned AS (

    SELECT
        start_time,
        stop_time,
        CAST(start_station_id AS STRING) AS start_station_id,
        start_station_name,
        start_station_latitude,
        start_station_longitude,
        CAST(end_station_id AS STRING) AS end_station_id,
        end_station_name,
        end_station_latitude,
        end_station_longitude,
        CAST(bike_id AS STRING) AS bike_id,
        user_type,
        CAST(birth_year AS INTEGER) AS birth_year,
        {{ map_gender('gender') }} AS gender,
        trip_duration_in_min,
        trip_duration AS trip_duration_in_sec        

    FROM base
    
)

SELECT * FROM cleaned
