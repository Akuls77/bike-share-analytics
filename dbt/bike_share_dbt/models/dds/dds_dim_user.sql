{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','dimension']
) }}

WITH base AS (

    SELECT DISTINCT
        user_type,
        gender,
        birth_year,
        age,
        age_group
    FROM {{ ref('cds_rides_enriched') }}

)

SELECT
    {{ generate_surrogate_key(['user_type','gender','birth_year']) }} AS user_sk,
    *
FROM base
