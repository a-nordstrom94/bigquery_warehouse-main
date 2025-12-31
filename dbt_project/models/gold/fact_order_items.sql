{{ config(materialized='view') }}

select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value as total_item_value,
    -- Order context
    o.order_status,
    o.order_purchase_timestamp,
    o.customer_id,
    -- Product context
    p.product_category_name_english,
    p.product_weight_g,
    -- Seller context
    s.seller_city,
    s.seller_state
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
left join {{ ref('dim_products') }} p
    on oi.product_id = p.product_id
left join {{ ref('dim_sellers') }} s
    on oi.seller_id = s.seller_id
