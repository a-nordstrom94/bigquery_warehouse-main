{{ config(materialized='table') }}

with bronze_geolocations as (
    select * from {{ source('bronze', 'geolocations') }}
)

select
    cast(geolocation_zip_code_prefix as string) as geolocation_zip_code_prefix,
    cast(geolocation_lat as float64) as geolocation_lat,
    cast(geolocation_lng as float64) as geolocation_lng,
    trim(cast(geolocation_city as string)) as geolocation_city,
    trim(cast(geolocation_state as string)) as geolocation_state
from bronze_geolocations