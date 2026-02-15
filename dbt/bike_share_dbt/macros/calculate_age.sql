{% macro calculate_age(birth_year_column) %}
    year(current_date) - {{ birth_year_column }}
{% endmacro %}
