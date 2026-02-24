{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={
        'field': 'order_purchase_timestamp',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['order_status', 'customer_id'] 
) }}

with orders_enriched as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        -- Delivery SLA timestamps
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        -- Computed delivery metrics (from int_orders__enriched)
        delivery_days,
        delivery_status,
        hours_to_approval,
        -- Financials
        total_items_price,
        total_freight,
        order_total,
        total_payment_value,
        payment_count,
        -- New Payment Dimensions for Dashboarding
        primary_payment_type,
        payment_methods_used,
        max_installments,
        credit_card_value,
        boleto_value,
        voucher_value,
        debit_card_value
    from {{ ref('int_orders__enriched') }}
),

reviews as (
    select 
        order_id,
        review_id,
        review_score,
        review_comment_message
    from {{ ref('int_orders__reviews') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        -- Delivery SLA timestamps
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        -- Computed delivery SLA metrics
        o.delivery_days,
        o.delivery_status,
        o.hours_to_approval,
        -- Financials
        o.total_items_price,
        o.total_freight,
        o.order_total as total_order_value,
        o.total_payment_value,
        o.payment_count,
        -- Payment Details
        o.primary_payment_type,
        o.payment_methods_used,
        o.max_installments,
        o.credit_card_value,
        o.boleto_value,
        o.voucher_value,
        o.debit_card_value,
        -- Review
        r.review_score,
        r.review_comment_message,
        r.review_id,
        {{ add_audit_columns() }}
    from orders_enriched o
    left join reviews r on o.order_id = r.order_id
    
    {% if is_incremental() %}
    where o.order_purchase_timestamp >= (
        select timestamp_sub(max(order_purchase_timestamp), interval 4 day) 
        from {{ this }}
    )
    {% endif %}
)

select * from final