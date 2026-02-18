{{
    config(
        materialized='ephemeral'
    )
}}

with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
)

select
    order_id,
    count(distinct product_id) as unique_products,
    sum(price) as total_item_value,
    sum(freight_value) as total_freight_value,
    avg(price) as avg_item_price,
    count(*) as total_items,
    sum(price + freight_value) as order_total
from order_items
group by order_id