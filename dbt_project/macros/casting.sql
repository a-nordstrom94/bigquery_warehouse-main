{% macro clean_string(column_name) %}
    trim(cast({{ column_name }} as string))
{% endmacro %}