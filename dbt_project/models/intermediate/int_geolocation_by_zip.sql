{{ config(materialized='ephemeral') }}

with base as (
    select *
    from {{ ref('stg_olist__geolocations') }}
),

deduped as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        avg(geolocation_lat) as latitude,
        avg(geolocation_lng) as longitude
    from base
    group by geolocation_zip_code_prefix
)

select * from deduped
