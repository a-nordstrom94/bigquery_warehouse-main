{{
    config(
        materialized='view'
    )
}}

select
    cast(product_category_name as string) as product_category_name,
    cast(product_category_name_english as string) as product_category_name_english,
    {{ add_audit_columns() }}
from {{ source('olist_bronze', 'product_category_name_translation') }}