{{ config(materialized='table') }}

with bronze_order_items as (
    select * from {{ source('bronze', 'order_items') }}
)

select
    cast(order_id as string) as order_id,
    cast(order_item_id as int64) as order_item_id,
    cast(product_id as string) as product_id,
    cast(seller_id as string) as seller_id,
    timestamp(cast(shipping_limit_date as string)) as shipping_limit_date,
    cast(price as float64) as price,
    cast(freight_value as float64) as freight_value
from bronze_order_items