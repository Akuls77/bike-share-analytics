with base as (

    select *
    from {{ ref('stg_rides') }}

)

select
    trip_duration_sec,
    trip_duration_in_min,
    start_at,
    stop_at,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    bike_id,
    user_type,
    gender,
    birth_year,

    datediff(year, to_date(birth_year::string || '-01-01'), current_date) as rider_age,

    case
        when datediff(year, to_date(birth_year::string || '-01-01'), current_date) < 18 then 'Under 18'
        when datediff(year, to_date(birth_year::string || '-01-01'), current_date) between 18 and 25 then '18-25'
        when datediff(year, to_date(birth_year::string || '-01-01'), current_date) between 26 and 35 then '26-35'
        when datediff(year, to_date(birth_year::string || '-01-01'), current_date) between 36 and 50 then '36-50'
        else '50+'
    end as age_group,

    date_trunc('day', start_at) as ride_date,
    date_trunc('month', start_at) as ride_month,
    date_trunc('hour', start_at) as ride_hour

from base
