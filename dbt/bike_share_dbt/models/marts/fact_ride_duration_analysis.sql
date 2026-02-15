with base as (

    select *
    from {{ ref('int_rides_enriched') }}

)

select
    ride_hour,
    user_type,
    gender,

    avg(trip_duration_sec) as avg_duration,
    min(trip_duration_sec) as min_duration,
    max(trip_duration_sec) as max_duration

from base
group by
    ride_hour,
    user_type,
    gender
