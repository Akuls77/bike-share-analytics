with source as (
    select *
    from {{ source('raw', 'raw_bike_rides') }}
),

renamed as (
    select
        "trip_duration"::integer              as trip_duration_sec,
        to_timestamp("start_time")            as start_at,
        to_timestamp("stop_time")             as stop_at,
        "start_station_id"::integer           as start_station_id,
        "start_station_name"                  as start_station_name,
        "start_station_latitude"::float       as start_station_latitude,
        "start_station_longitude"::float      as start_station_longitude,
        "end_station_id"::integer             as end_station_id,
        "end_station_name"                    as end_station_name,
        "end_station_latitude"::float         as end_station_latitude,
        "end_station_longitude"::float        as end_station_longitude,
        "bike_id"::integer                    as bike_id,
        "user_type"                           as user_type,
        "birth_year"::integer                 as birth_year,
        "gender"::integer                     as gender,
        "trip_duration_in_min"::integer       as trip_duration_in_min
    from source
)

select * from renamed