SELECT s.station_id
FROM {{ ref('dds_dim_station') }} s
LEFT JOIN {{ ref('dds_fact_rides') }} f
    ON s.station_sk = f.start_station_sk
WHERE f.ride_sk IS NULL
