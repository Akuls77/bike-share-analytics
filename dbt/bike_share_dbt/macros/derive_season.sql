{% macro derive_season(month_column) %}
    CASE
        WHEN {{ month_column }} IN (11, 12, 1, 2) THEN 'Winter'
        WHEN {{ month_column }} IN (3, 4, 5, 6) THEN 'Summer'
        WHEN {{ month_column }} IN (7, 8, 9, 10) THEN 'Monsoon'
        ELSE 'Unknown'
    END
{% endmacro %}
