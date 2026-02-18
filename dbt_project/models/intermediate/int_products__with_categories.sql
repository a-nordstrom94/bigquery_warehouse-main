{{
    config(
        materialized='ephemeral'
    )
}}

with products as (
    select * from {{ ref('stg_olist__products') }}
),

translations as (
    select * from {{ ref('stg_olist__product_category_translation') }}
)

select
    products.product_id,
    products.product_category_name,
    coalesce(translations.product_category_name_english, 'Unknown') as product_category_name_english,
    products.product_name_length,
    products.product_description_length,
    products.product_photos_qty,
    products.product_weight_g,
    products.product_length_cm,
    products.product_height_cm,
    products.product_width_cm
from products
left join translations 
    on products.product_category_name = translations.product_category_name