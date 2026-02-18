{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with sellers as (
    select * from {{ ref('dim_sellers') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
)

select
    sellers.seller_id,
    sellers.seller_city,
    sellers.seller_state,
    sellers.seller_lat,
    sellers.seller_lng,
    
    -- Sales metrics
    count(distinct order_items.order_id) as total_orders,
    count(*) as total_items_sold,
    sum(order_items.price) as total_revenue,
    avg(order_items.price) as avg_item_price,
    sum(order_items.freight_value) as total_freight,
    
    -- Product diversity
    count(distinct order_items.product_id) as unique_products_sold,
    count(distinct order_items.product_category_name_english) as unique_categories,
    
    -- Customer reach
    count(distinct order_items.customer_id) as unique_customers,
    
    -- Date metrics
    min(order_items.order_purchase_timestamp) as first_sale_date,
    max(order_items.order_purchase_timestamp) as last_sale_date,
    timestamp_diff(
        max(order_items.order_purchase_timestamp),
        min(order_items.order_purchase_timestamp),
        day
    ) as days_active,
    
    -- Performance tier
    case
        when sum(order_items.price) >= 10000 then 'Top Seller'
        when sum(order_items.price) >= 5000 then 'High Performer'
        when sum(order_items.price) >= 1000 then 'Average'
        else 'New/Low Volume'
    end as seller_performance_tier,
    
    {{ add_audit_columns() }}
    
from sellers
left join order_items using (seller_id)
group by
    sellers.seller_id,
    sellers.seller_city,
    sellers.seller_state,
    sellers.seller_lat,
    sellers.seller_lng