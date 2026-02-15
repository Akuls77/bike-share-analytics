with base as (

    select *
    from {{ ref('int_rides_enriched') }}

)

select
    start_station_id,
    end_station_id,
    count(*) as total_trips

from base
group by
    start_station_id,
    end_station_id
