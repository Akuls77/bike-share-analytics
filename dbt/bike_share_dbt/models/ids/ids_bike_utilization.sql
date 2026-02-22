{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    b.bike_id,
    COUNT(*) AS total_rides,
    AVG(f.trip_duration_in_min) AS avg_trip_duration_min,

    CASE 
        WHEN COUNT(*) > 1500 THEN 'High Usage'
        WHEN COUNT(*) BETWEEN 500 AND 1500 THEN 'Medium Usage'
        ELSE 'Low Usage'
    END AS usage_category

FROM {{ ref('dds_fact_rides') }} f

JOIN {{ ref('dds_dim_bike') }} b
    ON f.bike_sk = b.bike_sk

GROUP BY b.bike_id
