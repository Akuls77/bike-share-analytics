{% macro calculate_age(birth_year_column) %}
    EXTRACT(YEAR FROM CURRENT_DATE) - {{ birth_year_column }}
{% endmacro %}
