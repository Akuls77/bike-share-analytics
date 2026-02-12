{% macro age_bucket(birth_year_column) %}

    case
        when {{ birth_year_column }} is null then 'Unknown'
        when year(current_date) - {{ birth_year_column }} < 18 then '<18'
        when year(current_date) - {{ birth_year_column }} between 18 and 25 then '18-25'
        when year(current_date) - {{ birth_year_column }} between 26 and 40 then '26-40'
        when year(current_date) - {{ birth_year_column }} between 41 and 60 then '41-60'
        else '60+'
    end

{% endmacro %}
