{{
    config(
        materialized='ephemeral'
    )
}}

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

geo as (
    select * from {{ ref('int_geolocation_by_zip') }}
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.latitude as customer_lat,
    g.longitude as customer_lng
from customers c
left join geo g on c.customer_zip_code_prefix = g.zip_code_prefix