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

    CASE
        WHEN COUNT(*) > 20000 THEN 'High Usage'
        WHEN COUNT(*) BETWEEN 5000 AND 20000 THEN 'Medium Usage'
        ELSE 'Low Usage'
    END AS usage_bucket

FROM {{ ref('dds_fact_rides') }} f
JOIN {{ ref('dds_dim_user') }} u
    ON f.user_sk = u.user_sk
JOIN {{ ref('dds_dim_route') }} r
    ON f.route_sk = r.route_sk

GROUP BY
    u.user_type,
    u.gender,
    u.age_group
