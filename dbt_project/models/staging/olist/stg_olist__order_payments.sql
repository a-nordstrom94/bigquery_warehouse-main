{{
    config(
        materialized='view'
    )
}}

select
    cast(order_id as string) as order_id,
    cast(payment_sequential as int) as payment_sequential,
    cast(payment_type as string) as payment_type,
    cast(payment_installments as int) as payment_installments,
    cast(payment_value as float64) as payment_value,
    {{ add_audit_columns()}}
from {{ source('olist_bronze', 'order_payments') }}
