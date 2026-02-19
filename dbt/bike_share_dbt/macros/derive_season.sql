{% macro derive_season(month_column) %}
    CASE
        WHEN {{ month_column }} IN (12, 1, 2) THEN 'Winter'
        WHEN {{ month_column }} IN (3, 4, 5) THEN 'Spring'
        WHEN {{ month_column }} IN (6, 7, 8) THEN 'Summer'
        WHEN {{ month_column }} IN (9, 10, 11) THEN 'Fall'
        ELSE 'Unknown'
    END
{% endmacro %}
