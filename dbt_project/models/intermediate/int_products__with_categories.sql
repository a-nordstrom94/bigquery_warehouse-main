{{
    config(
        materialized='ephemeral'
    )
}}

with products as (
    -- Filter to current version only (dbt_valid_to IS NULL = active record in SCD2 snapshot)
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from {{ ref('snap_products') }}
    where dbt_valid_to is null
),

translations as (
    -- Deduplicate on the join key to prevent fan-out into products.
    -- The raw translation table has no uniqueness guarantee on product_category_name;
    -- duplicate rows here would multiply every product that has that category,
    -- propagating through fct_order_items and inflating product_performance totals.
    select
        product_category_name,
        -- Take the first English translation alphabetically for determinism.
        min(product_category_name_english) as product_category_name_english
    from {{ ref('stg_olist__product_category_translation') }}
    group by product_category_name
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