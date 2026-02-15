{% macro extract_time_dimensions(timestamp_column) %}
    date({{ timestamp_column }}) as ride_date,
    extract(hour from {{ timestamp_column }}) as ride_hour,
    extract(dayofweek from {{ timestamp_column }}) as ride_day_of_week,
    extract(month from {{ timestamp_column }}) as ride_month,
    extract(week from {{ timestamp_column }}) as ride_week
{% endmacro %}
