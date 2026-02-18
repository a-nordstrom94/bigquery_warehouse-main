{{
    config(
        materialized='ephemeral'
    )
}}

with orders_enriched as (
    select * from {{ ref('int_orders__enriched') }}
    where order_status not in ('canceled', 'unavailable')  -- Exclude cancelled orders
)

select
    customer_id,
    count(distinct order_id) as total_orders,
    sum(total_payment_value) as total_spend,
    avg(total_payment_value) as avg_order_value,
    min(order_purchase_timestamp) as first_order_date,
    max(order_purchase_timestamp) as last_order_date,
    
    -- Customer lifetime metrics
    timestamp_diff(
        max(order_purchase_timestamp),
        min(order_purchase_timestamp),
        day
    ) as days_since_first_order,
    
    -- Average delivery performance
    avg(delivery_days) as avg_delivery_days,
    
    -- Product variety
    sum(unique_products) as total_unique_products_ordered,
    
    -- Payment behavior
    avg(payment_count) as avg_payments_per_order
    
from orders_enriched
group by customer_id