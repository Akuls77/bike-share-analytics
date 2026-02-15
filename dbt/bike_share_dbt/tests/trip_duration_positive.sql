select *
from {{ ref('stg_rides') }}
where trip_duration_sec <= 0
