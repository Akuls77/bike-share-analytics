SELECT *
FROM {{ ref('dds_fact_rides') }}
WHERE trip_duration_in_sec < 0
