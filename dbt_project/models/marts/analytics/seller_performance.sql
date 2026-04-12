{{
    config(
        materialized='table'
    )
}}

with sellers as (
    select * from {{ ref('dim_sellers') }}
    where is_current = true
),

order_items as (
    select * from {{ ref('fct_order_items') }}
)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_lat,
    s.seller_lng,
    
    -- Sales metrics (Using updated fct_order_items aliases)
    count(distinct oi.order_id) as total_orders,
    count(oi.order_item_id) as total_items_sold,
    coalesce(sum(oi.item_price), 0) as total_revenue,
    coalesce(avg(oi.item_price), 0) as avg_item_price,
    coalesce(sum(oi.item_freight), 0) as total_freight,
    
    -- Product diversity
    count(distinct oi.product_id) as unique_products_sold,
    count(distinct oi.product_category_name_english) as unique_categories,
    
    -- Customer reach
    count(distinct oi.customer_id) as unique_customers,
    
    -- Date metrics
    min(oi.order_purchase_timestamp) as first_sale_date,
    max(oi.order_purchase_timestamp) as last_sale_date,
    timestamp_diff(
        max(oi.order_purchase_timestamp),
        min(oi.order_purchase_timestamp),
        day
    ) as days_active,

    -- Review
    avg(oi.review_score) as avg_review_score,

    -- Delivery SLA
    safe_divide(
        count(distinct case when oi.delivery_status = 'On Time' then oi.order_id end),
        count(distinct case when oi.delivery_status in ('On Time', 'Delayed') then oi.order_id end)
    ) as on_time_delivery_rate,

    -- Payment type order counts
    count(distinct case when oi.primary_payment_type = 'credit_card' then oi.order_id end) as credit_card_orders,
    count(distinct case when oi.primary_payment_type = 'boleto' then oi.order_id end) as boleto_orders,
    count(distinct case when oi.primary_payment_type = 'voucher' then oi.order_id end) as voucher_orders,
    count(distinct case when oi.primary_payment_type = 'debit_card' then oi.order_id end) as debit_card_orders,
    
    -- Performance tier (Logic using the new total_revenue alias)
    case
        when sum(oi.item_price) >= 10000 then 'Top Seller'
        when sum(oi.item_price) >= 5000 then 'High Performer'
        when sum(oi.item_price) >= 1000 then 'Average'
        else 'New/Low Volume'
    end as seller_performance_tier,
    
    {{ add_audit_columns() }}
    
from sellers s
left join order_items oi on s.seller_id = oi.seller_id
group by
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_lat,
    s.seller_lng