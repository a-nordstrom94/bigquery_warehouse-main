{{
    config(
        materialized='ephemeral'
    )
}}

with sellers as (
    -- Filter to current version only (dbt_valid_to IS NULL = active record in SCD2 snapshot)
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ ref('snap_sellers') }}
    where dbt_valid_to is null
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