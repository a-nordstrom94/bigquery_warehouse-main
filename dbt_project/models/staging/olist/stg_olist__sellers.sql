{{
    config(
        materialized='view'
    )
}}

select
    cast(seller_id as string) as seller_id,
    cast(seller_zip_code_prefix as string) as seller_zip_code_prefix,
    {{ clean_string('seller_city') }} as seller_city,
    {{ clean_string('seller_state') }} as seller_state,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'sellers') }}
