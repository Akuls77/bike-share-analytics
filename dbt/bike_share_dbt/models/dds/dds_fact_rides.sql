{{ config(
    materialized='table',
    schema='DDS',
    tags=['dds','fact']
) }}

WITH base AS (

    SELECT DISTINCT *
    FROM {{ ref('cds_rides_enriched') }}

)

SELECT
    {{ generate_surrogate_key([
        'bse.bike_id',
        'bse.start_time',
        'bse.stop_time',
        'bse.start_station_id',
        'bse.end_station_id'
    ]) }} AS ride_sk,

    u.user_sk,
    s_start.station_sk AS start_station_sk,
    s_end.station_sk AS end_station_sk,
    b.bike_sk,
    r.route_sk,

    bse.trip_duration,
    bse.trip_duration_in_min,
    bse.ride_date,
    bse.ride_hour,
    bse.ride_month,
    bse.ride_year,
    bse.season,
    bse.is_weekend

FROM base bse

LEFT JOIN {{ ref('dds_dim_user') }} u
    ON bse.user_type = u.user_type
    AND bse.gender = u.gender
    AND bse.birth_year = u.birth_year

LEFT JOIN {{ ref('dds_dim_station') }} s_start
    ON bse.start_station_id = s_start.station_id

LEFT JOIN {{ ref('dds_dim_station') }} s_end
    ON bse.end_station_id = s_end.station_id

LEFT JOIN {{ ref('dds_dim_bike') }} b
    ON bse.bike_id = b.bike_id

LEFT JOIN {{ ref('dds_dim_route') }} r
    ON bse.start_station_id = r.start_station_id
    AND bse.end_station_id = r.end_station_id
