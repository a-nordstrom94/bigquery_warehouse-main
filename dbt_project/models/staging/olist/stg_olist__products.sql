{{
    config(
        materialized='view'
    )
}}

select
    cast(product_id as string) as product_id,
    cast(product_category_name as string) as product_category_name,
    cast(product_name_length as int) as product_name_length, 
    cast(product_description_length as int) as product_description_length,
    cast(product_photos_qty as int) as product_photos_qty,
    cast(product_weight_g as int) as product_weight_g,
    cast(product_length_cm as int) as product_length_cm,
    cast(product_height_cm as int) as product_height_cm,
    cast(product_width_cm as int) as product_width_cm,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'products') }}