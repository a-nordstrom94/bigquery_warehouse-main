-- This test fails if it finds a 'delivered' order that has no matching items in fct_order_items
select
    o.order_id
from {{ ref('fct_orders') }} o
left join {{ ref('fct_order_items') }} i on o.order_id = i.order_id
where o.order_status = 'delivered'
  and i.order_id is null