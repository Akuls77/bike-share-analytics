SELECT *
FROM {{ ref('dds_fact_rides') }}
WHERE trip_duration_in_min < 0
