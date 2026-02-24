{{
    config(
        materialized='table'
    )
}}

with customers_standardized as (
    select * from {{ ref('int_customers__standardized') }}
),

order_history as (
    select * from {{ ref('int_customers__order_history') }}
),

final as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        c.customer_lat,
        c.customer_lng,
        
        coalesce(oh.total_orders, 0) as total_orders,
        oh.first_order_at,
        oh.last_order_at,
        coalesce(oh.lifetime_value, 0) as lifetime_value,
        oh.avg_review_score,
        oh.cancellation_rate,
        {{ add_audit_columns() }}
        
    from customers_standardized c
    left join order_history oh 
        on c.customer_id = oh.customer_id
)

select * from final