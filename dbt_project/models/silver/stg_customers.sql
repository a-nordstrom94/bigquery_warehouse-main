{{ config(materialized='table') }}

with bronze_customers as (
    select * from {{ source('bronze', 'customers') }}
)

select
    cast(customer_id as string) as customer_id,
    cast(customer_unique_id as string) as customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    trim(cast(customer_city as string)) as customer_city,
    trim(cast(customer_state as string)) as customer_state
from bronze_customers