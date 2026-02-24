{{
    config(
        materialized='ephemeral'
    )
}}

with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
)

select
    *,
    price + freight_value as item_total_value
from order_items