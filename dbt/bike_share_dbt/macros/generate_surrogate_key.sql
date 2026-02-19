{% macro generate_surrogate_key(columns) %}
    MD5(
        CONCAT(
            {% for col in columns %}
                COALESCE(CAST({{ col }} AS STRING), ''){% if not loop.last %}, '|' ,{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
