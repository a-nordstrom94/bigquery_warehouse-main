{{ config(materialized='view') }}

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    count(distinct o.order_id) as total_orders,
    sum(o.order_total) as total_spent,
    avg(o.order_total) as avg_order_value,
    min(o.order_purchase_timestamp) as first_order_date,
    max(o.order_purchase_timestamp) as last_order_date,
    avg(o.avg_review_score) as avg_customer_review_score
from {{ ref('dim_customers') }} c
left join {{ ref('fact_orders') }} o
    on c.customer_unique_id = o.customer_unique_id
group by
    c.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city
