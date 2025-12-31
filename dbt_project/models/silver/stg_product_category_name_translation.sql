{{ config(materialized='table') }}

with bronze_category_translation as (
    select * from {{ source('bronze', 'product_category_name_translation') }}
)

select
    trim(cast(product_category_name as string)) as product_category_name,
    trim(cast(product_category_name_english as string)) as product_category_name_english
from bronze_category_translation