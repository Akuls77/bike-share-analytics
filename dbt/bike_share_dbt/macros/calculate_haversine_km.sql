{% macro calculate_haversine_km(start_lat, start_lon, end_lat, end_lon) %}

    6371 * 2 * ASIN(
        SQRT(
            POWER(SIN(RADIANS({{ end_lat }} - {{ start_lat }}) / 2), 2) +
            COS(RADIANS({{ start_lat }})) *
            COS(RADIANS({{ end_lat }})) *
            POWER(SIN(RADIANS({{ end_lon }} - {{ start_lon }}) / 2), 2)
        )
    )

{% endmacro %}
