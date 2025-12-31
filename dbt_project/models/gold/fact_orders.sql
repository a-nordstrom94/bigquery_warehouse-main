{{ config(materialized='view') }}

select
    o.order_id,
    o.customer_id,
    o.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    date_diff(
        date(o.order_delivered_customer_date),
        date(o.order_purchase_timestamp),
        day
    ) as days_to_delivery,
    coalesce(oi.order_total, 0) as order_total,
    coalesce(oi.order_freight_total, 0) as order_freight_total,
    coalesce(oi.order_item_count, 0) as order_item_count,
    coalesce(op.payment_total, 0) as payment_total,
    coalesce(op.payment_count, 0) as payment_count,
    coalesce(r.avg_review_score, 0) as avg_review_score,
    coalesce(r.review_count, 0) as review_count
from {{ ref('stg_orders') }} o
left join (
    select
        order_id,
        sum(price) as order_total,
        sum(freight_value) as order_freight_total,
        count(*) as order_item_count
    from {{ ref('stg_order_items') }}
    group by order_id
) oi on o.order_id = oi.order_id
left join (
    select
        order_id,
        sum(payment_value) as payment_total,
        count(*) as payment_count
    from {{ ref('stg_order_payments') }}
    group by order_id
) op on o.order_id = op.order_id
left join (
    select
        order_id,
        avg(review_score) as avg_review_score,
        count(*) as review_count
    from {{ ref('stg_reviews') }}
    group by order_id
) r on o.order_id = r.order_id
