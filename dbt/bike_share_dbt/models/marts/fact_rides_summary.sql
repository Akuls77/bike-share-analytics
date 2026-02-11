with base as (

    select *
    from {{ ref('int_rides_enriched') }}

),

aggregated as (

    select
        ride_date,
        ride_hour,
        user_type,
        age_group,
        is_weekend,

        count(*) as total_rides,
        avg(trip_duration_sec) as avg_trip_duration_sec,
        median(trip_duration_sec) as median_trip_duration_sec

    from base
    group by
        ride_date,
        ride_hour,
        user_type,
        age_group,
        is_weekend

)

select * from aggregated
