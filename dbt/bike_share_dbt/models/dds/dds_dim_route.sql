{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','dimension']
) }}

WITH routes AS (

    SELECT DISTINCT
        start_station_id,
        end_station_id
    FROM {{ ref('cds_rides_enriched') }}

)

SELECT
    {{ generate_surrogate_key(['start_station_id','end_station_id']) }} AS route_sk,
    *
FROM routes
