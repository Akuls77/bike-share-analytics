with base as (

    select *
    from {{ ref('int_rides_enriched') }}

)

select
    user_type,
    gender,
    age_group,

    count(*) as total_rides,
    avg(trip_duration_sec) as avg_trip_duration_sec,
    avg(trip_duration_in_min) as avg_trip_duration_min

from base
group by
    user_type,
    gender,
    age_group
