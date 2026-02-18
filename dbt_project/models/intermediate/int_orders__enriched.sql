{{
    config(
        materialized='ephemeral'
    )
}}

with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

payments as (
    select * from {{ ref('int_orders__payments_pivoted') }}
),

items as (
    select * from {{ ref('int_orders__items_aggregated') }}
)

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp,
    orders.order_approved_at,
    orders.order_delivered_carrier_date,
    orders.order_delivered_customer_date,
    orders.order_estimated_delivery_date,
    
    -- Payment metrics
    coalesce(payments.total_payment_value, 0) as total_payment_value,
    coalesce(payments.payment_count, 0) as payment_count,
    payments.max_installments,
    payments.payment_methods_used,
    
    -- Item metrics
    coalesce(items.unique_products, 0) as unique_products,
    coalesce(items.total_item_value, 0) as total_item_value,
    coalesce(items.total_freight_value, 0) as total_freight_value,
    coalesce(items.avg_item_price, 0) as avg_item_price,
    coalesce(items.total_items, 0) as total_items,
    
    -- Calculated metrics
    coalesce(items.total_item_value, 0) + coalesce(items.total_freight_value, 0) as order_total,
    
    -- Delivery metrics
    case 
        when orders.order_delivered_customer_date is not null
        then timestamp_diff(
            orders.order_delivered_customer_date,
            orders.order_purchase_timestamp,
            day
        )
        else null
    end as delivery_days,
    
    case 
        when orders.order_delivered_customer_date is null then 'Not Delivered'
        when orders.order_delivered_customer_date <= orders.order_estimated_delivery_date then 'On Time'
        else 'Delayed'
    end as delivery_status,
    
    -- Time to approval
    case
        when orders.order_approved_at is not null
        then timestamp_diff(
            orders.order_approved_at,
            orders.order_purchase_timestamp,
            hour
        )
        else null
    end as hours_to_approval
    
from orders
left join payments using (order_id)
left join items using (order_id)