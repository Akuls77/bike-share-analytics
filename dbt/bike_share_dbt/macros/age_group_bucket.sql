{% macro age_group_bucket(age_column) %}
    CASE
        WHEN {{ age_column }} IS NULL THEN 'Unknown'
        WHEN {{ age_column }} < 18 THEN 'Under 18'
        WHEN {{ age_column }} BETWEEN 18 AND 25 THEN '18-25'
        WHEN {{ age_column }} BETWEEN 26 AND 35 THEN '26-35'
        WHEN {{ age_column }} BETWEEN 36 AND 50 THEN '36-50'
        WHEN {{ age_column }} > 50 THEN '50+'
        ELSE 'Unknown'
    END
{% endmacro %}
