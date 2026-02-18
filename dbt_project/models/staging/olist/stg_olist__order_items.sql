{{
    config(
        materialized='view'
    )
}}

select
    cast(order_id as string) as order_id,
    cast(order_item_id as int) as order_item_id,
    cast(product_id as string) as product_id,
    cast(seller_id as string) as seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as float64) as price,
    cast(freight_value as float64) as freight_value,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'order_items') }}