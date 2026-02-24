{{
    config(
        materialized='ephemeral'
    )
}}

with sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

geolocations as (
    select * from {{ ref('int_geolocation_by_zip') }}
)

select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude as seller_lat,
    g.longitude as seller_lng
from sellers s
left join geolocations g 
    on s.seller_zip_code_prefix = g.zip_code_prefix