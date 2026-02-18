{% macro add_audit_columns() %}
    current_timestamp() as _dbt_loaded_at,
    '{{ invocation_id }}' as _dbt_run_id
{% endmacro %}

{% macro test_row_count_match(model, upstream_model) %}
    select count(*) as row_count_diff
    from (
        select count(*) as cnt from {{ model }}
        union all
        select -count(*) as cnt from {{ upstream_model }}
    )
    having sum(cnt) != 0
{% endmacro %}