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
        total_reviews,
        on_time_orders,
        on_time_delivery_rate,
        total_items_ordered,
        credit_card_spend,
        boleto_spend,
        voucher_spend,
        debit_card_spend
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
    total_items_ordered,
    avg_review_score,
    total_reviews,
    -- Delivery SLA
    on_time_orders,
    on_time_delivery_rate,
    -- Payment type spend
    credit_card_spend,
    boleto_spend,
    voucher_spend,
    debit_card_spend,
    case
        when total_orders >= 5 then 'Loyal'
        when total_orders >= 2 then 'Repeat'
        else 'One-time'
    end as customer_segment,
    cancellation_rate,
    {{ add_audit_columns() }}
from customer_base