select *
from {{ ref('fact_ride_activity_trends') }}
where total_rides < 0
