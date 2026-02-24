{{
    config(
        materialized='ephemeral'
    )
}}

with payments as (
    select * from {{ ref('stg_olist__order_payments') }}
)

select
    order_id,
    -- Basic Aggregates
    sum(payment_value) as total_payment_value,
    count(*) as payment_count,
    max(payment_installments) as max_payment_installments,
    string_agg(distinct payment_type, ', ' order by payment_type) as payment_methods_used,
    sum(case when payment_type = 'credit_card' then payment_value else 0 end) as credit_card_value,
    sum(case when payment_type = 'boleto' then payment_value else 0 end) as boleto_value,
    sum(case when payment_type = 'voucher' then payment_value else 0 end) as voucher_value,
    sum(case when payment_type = 'debit_card' then payment_value else 0 end) as debit_card_value,
    approx_top_count(payment_type, 1)[offset(0)].value as primary_payment_type

from payments
group by 1