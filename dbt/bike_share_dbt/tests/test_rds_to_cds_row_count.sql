WITH rds_count AS (

    SELECT COUNT(*) AS cnt
    FROM {{ source('rds', 'raw_bike_rides') }}

),

cds_count AS (

    SELECT COUNT(*) AS cnt
    FROM {{ ref('cds_rides_cleaned') }}

)

SELECT *
FROM rds_count r
JOIN cds_count c
    ON r.cnt != c.cnt
