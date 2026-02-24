{{
    config(materialized='ephemeral')
}}

with orders as (
    select * from {{ ref('int_orders__enriched') }}
),

reviews as (
    select * from {{ ref('int_orders__reviews') }}
),

customer_history as (
    select
        o.customer_id,
        count(distinct o.order_id) as total_orders,
        
        safe_divide(
            count(distinct case when o.order_status = 'canceled' then o.order_id end),
            count(distinct o.order_id)
        ) as cancellation_rate,
        
        min(o.order_purchase_timestamp) as first_order_at,
        max(o.order_purchase_timestamp) as last_order_at,
        
        -- Using 'order_total' from enriched model (which is price + freight)
        sum(case when o.order_status != 'canceled' then o.order_total else 0 end) as lifetime_value,
        
        avg(r.review_score) as avg_review_score
    from orders o
    left join reviews r on o.order_id = r.order_id
    group by 1
)

select * from customer_history