{{
    config(
        materialized='table'
    )
}}

-- dim_sellers is a SCD Type 2 dimension. Each row represents one version of a seller's
-- attributes. Use is_current = true to get the current state of each seller.
-- dbt_scd_id is the surrogate key that uniquely identifies a specific version.

with sellers_snapshot as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        dbt_valid_to is null as is_current
    from {{ ref('snap_sellers') }}
),

geo as (
    select * from {{ ref('int_geolocation_by_zip') }}
)

select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude as seller_lat,
    g.longitude as seller_lng,
    -- SCD Type 2 metadata
    s.dbt_scd_id,
    s.dbt_valid_from,
    s.dbt_valid_to,
    s.is_current,
    {{ add_audit_columns() }}
from sellers_snapshot s
left join geo g on s.seller_zip_code_prefix = g.zip_code_prefix