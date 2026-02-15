{% macro age_group_bucket(age_column) %}
    case
        when {{ age_column }} < 18 then 'Under 18'
        when {{ age_column }} between 18 and 25 then '18-25'
        when {{ age_column }} between 26 and 35 then '26-35'
        when {{ age_column }} between 36 and 50 then '36-50'
        else '50+'
    end
{% endmacro %}
