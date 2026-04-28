-- This CTE finds what products use what payment type and what customers also use it

with payment_information as (
select
op.payment_type,
op.payment_installments,
o.customer_id as order_customer_id,
oi.product_id,
case
  when op.payment_installments = 1 then 'onetime purchase'
  when op.payment_installments > 1 then 'multiple installments'
  else 'not paid for or error'
  end as type_of_installment
from
order_payments as op
join order_items as oi on op.order_id = oi.order_id
join orders as o on oi.order_id = o.order_id
where payment_installments = $1
order by payment_type
)
select
    payment_type,
    payment_installments,
    order_customer_id,
    product_id,
    type_of_installment
from payment_information
