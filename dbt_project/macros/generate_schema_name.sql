{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# No custom schema - use default (olist_silver) #}
        {{ default_schema }}
    
    {%- elif custom_schema_name == 'gold' or custom_schema_name == 'core' or custom_schema_name == 'analytics' -%}
        {# Gold/Marts models go to olist_gold #}
        {{ env_var('GOLD_DATASET', 'olist_gold') }}
    
    {%- else -%}
        {# Any other custom schema #}
        {{ custom_schema_name | trim }}
    
    {%- endif -%}
{%- endmacro %}