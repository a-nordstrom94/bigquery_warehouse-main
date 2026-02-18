{{
    config(
        materialized='view'
    )
}}

select
    cast(geolocation_zip_code_prefix as string) as geolocation_zip_code_prefix,
    cast(geolocation_lat as float64) as geolocation_lat,
    cast(geolocation_lng as float64) as geolocation_lng,
    {{ clean_string('geolocation_city') }} as geolocation_city,
    {{ clean_string('geolocation_state') }} as geolocation_state,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'geolocations') }}