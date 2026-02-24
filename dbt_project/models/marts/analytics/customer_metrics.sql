{{
    config(
        materialized='table'
    )
}}

with customer_base as (
    select
        customer_id,
        customer_city,
        customer_state,
        total_orders,
        first_order_at as first_order_date,
        last_order_at as last_order_date,
        lifetime_value as total_spent,
        avg_review_score,
        cancellation_rate,
        {{ add_audit_columns() }}
    from {{ ref('dim_customers') }}
)

select
    customer_id,
    coalesce(customer_city, 'Unknown') as customer_city,
    coalesce(customer_state, 'Unknown') as customer_state,
    total_orders,
    first_order_date,
    last_order_date,
    date_diff(date(last_order_date), date(first_order_date), day) as customer_lifetime_days,
    total_spent,
    safe_divide(total_spent, total_orders) as avg_order_value,
    avg_review_score,
    case
        when total_orders >= 5 then 'Loyal'
        when total_orders >= 2 then 'Repeat'
        else 'One-time'
    end as customer_segment,
    cancellation_rate,
    {{ add_audit_columns() }}
from customer_base