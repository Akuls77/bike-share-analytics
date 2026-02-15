select
    "Trip Duration" as trip_duration_sec,
    "Trip_Duration_in_min" as trip_duration_in_min,
    "Start Time"::timestamp as start_at,
    "Stop Time"::timestamp as stop_at,
    "Start Station ID" as start_station_id,
    "Start Station Name" as start_station_name,
    "Start Station Latitude" as start_station_latitude,
    "Start Station Longitude" as start_station_longitude,
    "End Station ID" as end_station_id,
    "End Station Name" as end_station_name,
    "End Station Latitude" as end_station_latitude,
    "End Station Longitude" as end_station_longitude,
    "Bike ID" as bike_id,
    "User Type" as user_type,
    "Birth Year" as birth_year,
    "Gender" as gender
from {{ source('bike_raw', 'RAW_BIKE_RIDES') }}
where "Stop Time" >= "Start Time"
