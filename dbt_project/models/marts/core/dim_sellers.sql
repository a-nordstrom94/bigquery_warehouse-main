{{
    config(
        materialized='table',
        schema='gold'
    )
}}

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_lat,
    seller_lng,
    
    {{ add_audit_columns() }}
    
from {{ ref('int_sellers__with_locations') }}