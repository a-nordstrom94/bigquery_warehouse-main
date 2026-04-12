{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('dim_products') }}
    where is_current = true
),

order_items as (
    select * from {{ ref('fct_order_items') }}
)

select
    p.product_id,
    p.product_category_name_english,
    p.product_weight_g,
    p.product_volume_cm3,
    count(distinct oi.order_id) as total_orders,
    count(*) as total_items_sold,
    coalesce(sum(oi.item_price), 0) as total_revenue,
    coalesce(avg(oi.item_price), 0) as avg_price,
    coalesce(sum(oi.item_freight), 0) as total_freight,
    coalesce(sum(oi.item_total_with_freight), 0) as total_revenue_with_freight,
    
    -- Seller diversity
    count(distinct oi.seller_id) as unique_sellers,
    
    -- Date metrics
    min(oi.order_purchase_timestamp) as first_sale_date,
    max(oi.order_purchase_timestamp) as last_sale_date,
    
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
    
    -- Performance indicators
    case
        when count(distinct oi.order_id) >= 100 then 'Bestseller'
        when count(distinct oi.order_id) >= 50 then 'Popular'
        when count(distinct oi.order_id) >= 10 then 'Moderate'
        else 'Low Volume'
    end as product_performance_tier,
    
    {{ add_audit_columns() }}
    
from products p
left join order_items oi on p.product_id = oi.product_id
group by
    p.product_id,
    p.product_category_name_english,
    p.product_weight_g,
    p.product_volume_cm3