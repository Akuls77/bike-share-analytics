{% macro map_gender(gender_column) %}
    CASE
        WHEN {{ gender_column }} = 1 THEN 'Male'
        WHEN {{ gender_column }} = 2 THEN 'Female'
        WHEN {{ gender_column }} = 0 THEN 'Unknown'
        ELSE 'Unknown'
    END
{% endmacro %}
