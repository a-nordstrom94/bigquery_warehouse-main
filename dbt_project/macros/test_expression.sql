{% test expression(model, column_name, expression) %}

    select *
    from {{ model }}
    where not ({{ expression }})

{% endtest %}