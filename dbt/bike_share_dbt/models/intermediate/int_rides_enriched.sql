with base as (

    select *
    from {{ ref('stg_rides') }}

),

derived as (

    select

        -- primary ride info
        trip_duration_sec,
        trip_duration_in_min,
        start_at,
        stop_at,

        -- date features
        cast(start_at as date) as ride_date,
        extract(hour from start_at) as ride_hour,
        dayname(start_at) as ride_day_name,
        case 
            when dayofweek(start_at) in (0,6) then 1
            else 0
        end as is_weekend,

        -- rider info
        user_type,
        gender,
        birth_year,

        -- calculated age
        case
            when birth_year is not null 
            then year(current_date) - birth_year
            else null
        end as rider_age,

        -- age bucket macro
        {{ age_bucket('birth_year') }} as age_group,

        -- station info
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,

        bike_id

    from base

)

select * from derived
