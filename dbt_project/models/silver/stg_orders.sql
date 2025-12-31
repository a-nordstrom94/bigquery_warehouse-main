{{ config(materialized='table') }}

with bronze_orders as (
    select * from {{ source('bronze', 'orders') }}
),
bronze_customers as (
    select * from {{ source('bronze', 'customers') }}
)

select
    cast(o.order_id as string) as order_id,
    cast(o.customer_id as string) as customer_id,
    c.customer_unique_id,
    cast(o.order_status as string) as order_status,
    timestamp(cast(o.order_purchase_timestamp as string)) as order_purchase_timestamp,

    case when cast(o.order_approved_at as string) = '' then null
         else timestamp(cast(o.order_approved_at as string)) end as order_approved_at,

    case when cast(o.order_delivered_carrier_date as string) = '' then null
         else timestamp(cast(o.order_delivered_carrier_date as string)) end
         as order_delivered_carrier_date,

    case when cast(o.order_delivered_customer_date as string) = '' then null
         else timestamp(cast(o.order_delivered_customer_date as string)) end
         as order_delivered_customer_date,

    timestamp(cast(o.order_estimated_delivery_date as string)) as order_estimated_delivery_date

from bronze_orders o
left join bronze_customers c
    on o.customer_id = c.customer_id
