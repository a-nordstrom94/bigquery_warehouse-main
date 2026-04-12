{{
    config(
        materialized='table'
    )
}}

-- dim_products is a SCD Type 2 dimension. Each row represents one version of a product's
-- attributes. Use is_current = true to get the current state of each product.
-- dbt_scd_id is the surrogate key that uniquely identifies a specific version.

with products_snapshot as (
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        dbt_valid_to is null as is_current
    from {{ ref('snap_products') }}
),

translations as (
    select
        product_category_name,
        min(product_category_name_english) as product_category_name_english
    from {{ ref('stg_olist__product_category_translation') }}
    group by product_category_name
)

select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english, 'Unknown') as product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    (p.product_length_cm * p.product_height_cm * p.product_width_cm) as product_volume_cm3,
    -- SCD Type 2 metadata
    p.dbt_scd_id,
    p.dbt_valid_from,
    p.dbt_valid_to,
    p.is_current,
    {{ add_audit_columns() }}
from products_snapshot p
left join translations t on p.product_category_name = t.product_category_name