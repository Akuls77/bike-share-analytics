{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','dimension']
) }}

SELECT DISTINCT
    {{ generate_surrogate_key(['bike_id']) }} AS bike_sk,
    bike_id
FROM {{ ref('cds_rides_enriched') }}
