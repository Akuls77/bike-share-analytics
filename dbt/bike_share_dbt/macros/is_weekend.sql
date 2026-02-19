{% macro is_weekend(date_column) %}
    CASE
        WHEN DAYOFWEEK({{ date_column }}) IN (1,7) THEN TRUE
        ELSE FALSE
    END
{% endmacro %}
