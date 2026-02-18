{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            "field": "order_purchase_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['order_status', 'customer_id']
    )
}}


select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    
    -- Payment metrics
    total_payment_value,
    payment_count,
    max_installments,
    payment_methods_used,
    
    -- Item metrics
    unique_products,
    total_item_value,
    total_freight_value,
    avg_item_price,
    total_items,
    order_total,
    
    -- Delivery metrics
    delivery_days,
    delivery_status,
    hours_to_approval,
    
    {{ add_audit_columns() }}
    
from {{ ref('int_orders__enriched') }}

{% if is_incremental() %}
where order_purchase_timestamp > (select max(order_purchase_timestamp) from {{ this }})
{% endif %}