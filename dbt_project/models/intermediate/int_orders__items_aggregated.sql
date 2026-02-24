{{
    config(
        materialized='ephemeral'
    )
}}

with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

aggregated as (
    select
        order_id,
        count(*) as total_items,
        count(distinct product_id) as unique_products,
        count(distinct seller_id) as unique_sellers,
        sum(price) as total_items_price,
        sum(freight_value) as total_freight,
        safe_divide(sum(price), count(*)) as avg_item_price
    from order_items
    group by 1
)

select * from aggregated