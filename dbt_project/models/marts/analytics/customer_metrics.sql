{{
    config(
        materialized='table',
        dataset='olist_gold'
    )
}}

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) as total_orders,
        MIN(order_purchase_timestamp) as first_order_date,
        MAX(order_purchase_timestamp) as last_order_date,
        SUM(total_payment_value) as total_spent,
        AVG(total_payment_value) as avg_order_value,
        COUNT(DISTINCT CASE 
            WHEN order_status = 'delivered' 
            THEN order_id 
        END) as delivered_orders,
        COUNT(DISTINCT CASE 
            WHEN order_status = 'canceled' 
            THEN order_id 
        END) as canceled_orders
    FROM {{ ref('fct_orders') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),

customer_reviews AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT r.review_id) as total_reviews,
        AVG(r.review_score) as avg_review_score
    FROM {{ ref('fct_orders') }} o
    LEFT JOIN {{ ref('stg_olist__reviews') }} r
        ON o.order_id = r.order_id
    WHERE o.customer_id IS NOT NULL
    GROUP BY o.customer_id
),

customer_info AS (
    SELECT
        customer_id,
        customer_city,
        customer_state
    FROM {{ ref('dim_customers') }}
)

SELECT
    co.customer_id,
    COALESCE(ci.customer_city, 'Unknown') as customer_city,
    COALESCE(ci.customer_state, 'Unknown') as customer_state,
    co.total_orders,
    co.first_order_date,
    co.last_order_date,
    DATE_DIFF(co.last_order_date, co.first_order_date, DAY) as customer_lifetime_days,
    co.total_spent,
    co.avg_order_value,
    co.delivered_orders,
    co.canceled_orders,
    SAFE_DIVIDE(co.canceled_orders, co.total_orders) as cancellation_rate,
    COALESCE(cr.total_reviews, 0) as total_reviews,
    cr.avg_review_score,
    CASE
        WHEN co.total_orders >= 5 THEN 'Loyal'
        WHEN co.total_orders >= 2 THEN 'Repeat'
        ELSE 'One-time'
    END as customer_segment
FROM customer_orders co  -- ← Start from orders (source of truth)
LEFT JOIN customer_info ci ON co.customer_id = ci.customer_id  -- ← Join to dim
LEFT JOIN customer_reviews cr ON co.customer_id = cr.customer_id