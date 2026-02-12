select *
from {{ ref('stg_rides') }}
where trip_duration_in_min <= 0
