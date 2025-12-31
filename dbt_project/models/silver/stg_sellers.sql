{{ config(materialized='table') }}

with bronze_sellers as (
    select * from {{ source('bronze', 'sellers') }}
)

select
    cast(seller_id as string) as seller_id,
    cast(seller_zip_code_prefix as string) as seller_zip_code_prefix,
    trim(cast(seller_city as string)) as seller_city,
    trim(cast(seller_state as string)) as seller_state
from bronze_sellers