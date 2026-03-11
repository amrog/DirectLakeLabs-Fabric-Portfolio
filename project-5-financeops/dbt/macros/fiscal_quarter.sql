-- fiscal_quarter.sql
-- Derives fiscal quarter from a month number.
-- Reusable across models. Assumes calendar-year fiscal alignment.

{% macro fiscal_quarter(month_column) %}
    case
        when {{ month_column }} between 1 and 3 then 'Q1'
        when {{ month_column }} between 4 and 6 then 'Q2'
        when {{ month_column }} between 7 and 9 then 'Q3'
        when {{ month_column }} between 10 and 12 then 'Q4'
    end
{% endmacro %}
