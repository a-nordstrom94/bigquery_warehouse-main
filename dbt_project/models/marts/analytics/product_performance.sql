{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with products as (
    select * from {{ ref('dim_products') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
)

select
    products.product_id,
    products.product_category_name_english,
    products.product_weight_g,
    products.product_volume_cm3,
    
    -- Sales metrics
    count(distinct order_items.order_id) as total_orders,
    count(*) as total_items_sold,
    sum(order_items.price) as total_revenue,
    avg(order_items.price) as avg_price,
    sum(order_items.freight_value) as total_freight,
    sum(order_items.item_total) as total_revenue_with_freight,
    
    -- Seller diversity
    count(distinct order_items.seller_id) as unique_sellers,
    
    -- Date metrics
    min(order_items.order_purchase_timestamp) as first_sale_date,
    max(order_items.order_purchase_timestamp) as last_sale_date,
    
    -- Performance indicators
    case
        when count(distinct order_items.order_id) >= 100 then 'Bestseller'
        when count(distinct order_items.order_id) >= 50 then 'Popular'
        when count(distinct order_items.order_id) >= 10 then 'Moderate'
        else 'Low Volume'
    end as product_performance_tier,
    
    {{ add_audit_columns() }}
    
from products
left join order_items using (product_id)
group by
    products.product_id,
    products.product_category_name_english,
    products.product_weight_g,
    products.product_volume_cm3