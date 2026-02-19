{{ config(
    materialized='table',
    schema='IDS',
    tags=['ids','insight']
) }}

SELECT
    u.user_type,
    u.gender,
    u.age_group,
    COUNT(*) AS total_rides,
    AVG(f.trip_duration_in_min) AS avg_duration_min
FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_user') }} u
    ON f.user_sk = u.user_sk
GROUP BY
    u.user_type,
    u.gender,
    u.age_group
