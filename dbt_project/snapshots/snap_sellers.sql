{% snapshot snap_sellers %}

{{
    config(
        target_schema=env_var('SNAPSHOTS_DATASET', 'olist_snapshots'),
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_zip_code_prefix', 'seller_city', 'seller_state'],
        invalidate_hard_deletes=True
    )
}}

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from {{ ref('stg_olist__sellers') }}

{% endsnapshot %}
