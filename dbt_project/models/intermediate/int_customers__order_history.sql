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
        
        avg(r.review_score) as avg_review_score,
        count(r.review_id) as total_reviews,

        -- Delivery SLA
        count(distinct case when o.delivery_status = 'On Time' then o.order_id end) as on_time_orders,
        safe_divide(
            count(distinct case when o.delivery_status = 'On Time' then o.order_id end),
            count(distinct case when o.delivery_status in ('On Time', 'Delayed') then o.order_id end)
        ) as on_time_delivery_rate,

        -- Order volume
        sum(case when o.order_status != 'canceled' then o.total_items else 0 end) as total_items_ordered,

        -- Payment type spend
        sum(case when o.order_status != 'canceled' then o.credit_card_value else 0 end) as credit_card_spend,
        sum(case when o.order_status != 'canceled' then o.boleto_value else 0 end) as boleto_spend,
        sum(case when o.order_status != 'canceled' then o.voucher_value else 0 end) as voucher_spend,
        sum(case when o.order_status != 'canceled' then o.debit_card_value else 0 end) as debit_card_spend
    from orders o
    left join reviews r on o.order_id = r.order_id
    group by 1
)

select * from customer_history