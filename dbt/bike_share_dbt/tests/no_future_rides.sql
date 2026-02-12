select *
from {{ ref('stg_rides') }}
where start_at > current_timestamp
