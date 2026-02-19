{{ config(
    materialized='table',
    schema='RDS',
    tags=['rds']
) }}

SELECT *
FROM {{ source('rds', 'raw_bike_rides') }}
