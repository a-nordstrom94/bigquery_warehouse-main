{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id'],
        incremental_strategy='merge',
        partition_by={
            "field": "order_purchase_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['order_status', 'customer_id']
    )
}}


with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('int_orders__enriched') }}
),

products as (
    select * from {{ ref('int_products__with_categories') }}
),

sellers as (
    select * from {{ ref('int_sellers__with_locations') }}
)

select
    order_items.order_id,
    order_items.order_item_id,
    order_items.product_id,
    order_items.seller_id,
    
    -- Order context
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp,
    
    -- Product details
    products.product_category_name_english,
    
    -- Seller details  
    sellers.seller_city,
    sellers.seller_state,
    
    -- Item metrics
    order_items.price,
    order_items.freight_value,
    order_items.price + order_items.freight_value as item_total,
    
    {{ add_audit_columns() }}
    
from order_items
left join orders using (order_id)
left join products using (product_id)
left join sellers using (seller_id)