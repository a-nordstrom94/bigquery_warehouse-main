{{
    config(
        materialized='ephemeral'
    )
}}

with sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

geolocations as (
    select 
        geolocation_zip_code_prefix,
        -- Take first geolocation per zip (they may have duplicates)
        any_value(geolocation_lat) as geolocation_lat,
        any_value(geolocation_lng) as geolocation_lng,
        any_value(geolocation_city) as geolocation_city,
        any_value(geolocation_state) as geolocation_state
    from {{ ref('stg_olist__geolocations') }}
    group by geolocation_zip_code_prefix
)

select
    sellers.seller_id,
    sellers.seller_zip_code_prefix,
    sellers.seller_city,
    sellers.seller_state,
    geolocations.geolocation_lat as seller_lat,
    geolocations.geolocation_lng as seller_lng
from sellers
left join geolocations 
    on sellers.seller_zip_code_prefix = geolocations.geolocation_zip_code_prefix