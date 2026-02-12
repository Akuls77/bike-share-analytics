select *
from {{ ref('stg_rides') }}
where stop_at < start_at
