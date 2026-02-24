{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('int_products__with_categories') }}
),

final as (
    select
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        (p.product_length_cm * p.product_height_cm * p.product_width_cm) as product_volume_cm3,
        {{ add_audit_columns() }}
    from products p
)

select * from final