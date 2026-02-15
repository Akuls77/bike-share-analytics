{{ config(
    materialized='incremental',
    unique_key='ride_date'
) }}

with base as (

    select *
    from {{ ref('int_rides_enriched') }}

    {% if is_incremental() %}
        where ride_date > (select max(ride_date) from {{ this }})
    {% endif %}

)

select
    ride_date,
    ride_month,
    ride_hour,
    dayname(ride_date) as weekday_name,
    count(*) as total_rides,
    avg(trip_duration_sec) as avg_trip_duration_sec

from base
group by
    ride_date,
    ride_month,
    ride_hour,
    weekday_name
