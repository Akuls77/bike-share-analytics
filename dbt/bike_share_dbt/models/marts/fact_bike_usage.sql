with base as (

    select *
    from {{ ref('int_rides_enriched') }}

)

select
    bike_id,
    count(*) as total_trips,
    avg(trip_duration_sec) as avg_trip_duration

from base
group by
    bike_id
