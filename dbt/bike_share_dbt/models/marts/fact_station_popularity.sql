with base as (

    select *
    from {{ ref('int_rides_enriched') }}

)

select
    start_station_id,
    start_station_name,
    count(*) as total_departures

from base
group by
    start_station_id,
    start_station_name
