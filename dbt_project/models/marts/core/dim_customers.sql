{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

order_history as (
    select * from {{ ref('int_customers__order_history') }}
)

select
    customers.customer_id,
    customers.customer_unique_id,
    customers.customer_zip_code_prefix,
    customers.customer_city,
    customers.customer_state,
    
    -- Order history metrics
    coalesce(order_history.total_orders, 0) as total_orders,
    coalesce(order_history.total_spend, 0) as total_spend,
    coalesce(order_history.avg_order_value, 0) as avg_order_value,
    order_history.first_order_date,
    order_history.last_order_date,
    coalesce(order_history.days_since_first_order, 0) as days_since_first_order,
    
    -- Customer segmentation
    case
        when order_history.total_orders >= 5 then 'Loyal'
        when order_history.total_orders >= 2 then 'Repeat'
        when order_history.total_orders = 1 then 'One-time'
        else 'No Orders'
    end as customer_segment,
    
    {{ add_audit_columns() }}
    
from customers
left join order_history using (customer_id)