{% macro calculate_age(birth_year_column) %}
    CASE
        WHEN {{ birth_year_column }} IS NULL THEN NULL
        WHEN {{ birth_year_column }} < 1900 THEN NULL
        WHEN {{ birth_year_column }} > EXTRACT(YEAR FROM CURRENT_DATE) THEN NULL
        ELSE EXTRACT(YEAR FROM CURRENT_DATE) - {{ birth_year_column }}
    END
{% endmacro %}
