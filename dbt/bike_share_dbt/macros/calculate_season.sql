{% macro calculate_season(month_column) %}
    case
        when {{ month_column }} in (12,1,2) then 'Winter'
        when {{ month_column }} in (3,4,5) then 'Spring'
        when {{ month_column }} in (6,7,8) then 'Summer'
        else 'Fall'
    end
{% endmacro %}
