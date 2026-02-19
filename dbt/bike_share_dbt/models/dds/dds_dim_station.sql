{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','dimension']
) }}

WITH stations AS (

    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_station_latitude AS latitude,
        start_station_longitude AS longitude
    FROM {{ ref('cds_rides_enriched') }}

    UNION

    SELECT
        end_station_id,
        end_station_name,
        end_station_latitude,
        end_station_longitude
    FROM {{ ref('cds_rides_enriched') }}

)

SELECT DISTINCT
    {{ generate_surrogate_key(['station_id']) }} AS station_sk,
    *
FROM stations
