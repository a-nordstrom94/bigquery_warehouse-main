{% macro get_bronze_table(table_name) %}
    {{ source('olist_bronze', table_name) }}
{% endmacro %}