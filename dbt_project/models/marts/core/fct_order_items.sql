{{ config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={
        'field': 'order_purchase_timestamp',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['order_status', 'product_id']
) }}

with order_items as (
    select 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        item_total_value
    from {{ ref('int_orders__items_enriched') }}
),

orders as (
    select 
        order_id, 
        customer_id,
        order_purchase_timestamp,
        order_status 
    from {{ ref('int_orders__enriched') }}
),

products as (
    select 
        product_id,
        product_category_name_english 
    from {{ ref('int_products__with_categories') }} 
),

sellers as (
    select
        seller_id,
        seller_city,
        seller_state
    from {{ ref('int_sellers__with_locations') }}
),

final as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        p.product_category_name_english,
        oi.seller_id,
        s.seller_city,
        s.seller_state,
        o.customer_id,
        oi.shipping_limit_date,
        oi.price as item_price,
        oi.freight_value as item_freight,
        oi.item_total_value as item_total_with_freight, 
        o.order_purchase_timestamp,
        o.order_status,
        {{ add_audit_columns() }}
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    left join products p on oi.product_id = p.product_id
    left join sellers s on oi.seller_id = s.seller_id

    {% if is_incremental() %}
    where o.order_purchase_timestamp >= (
        select timestamp_sub(max(order_purchase_timestamp), interval 4 day) 
        from {{ this }}
    )
    {% endif %}
)

select * from final