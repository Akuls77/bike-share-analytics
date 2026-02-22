SELECT *
FROM {{ ref('dds_fact_rides') }}
WHERE ride_date > CURRENT_DATE
