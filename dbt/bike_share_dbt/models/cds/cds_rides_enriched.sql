{{ config(
    materialized='view',
    schema='CDS',
    tags=['cds']
) }}

WITH base AS (

    SELECT *
    FROM {{ ref('cds_rides_cleaned') }}

),

derived AS (

    SELECT
        *,
        {{ calculate_age('birth_year') }} AS age,
        EXTRACT(HOUR FROM start_time) AS ride_hour,
        CAST(start_time AS DATE) AS ride_date,
        EXTRACT(MONTH FROM start_time) AS ride_month,
        EXTRACT(YEAR FROM start_time) AS ride_year

    FROM base

),

final AS (

    SELECT
        *,
        {{ age_group_bucket('age') }} AS age_group,
        {{ derive_season('ride_month') }} AS season,
        {{ is_weekend('ride_date') }} AS is_weekend

    FROM derived

)

SELECT * FROM final
