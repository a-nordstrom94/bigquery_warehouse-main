{{ 
    config(
        materialized='view'
        ) 
}}

select
    cast(customer_id as string) as customer_id,
    cast(customer_unique_id as string) as customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    {{ clean_string('customer_city') }} as customer_city,
    {{ clean_string('customer_state') }} as customer_state,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'customers') }}