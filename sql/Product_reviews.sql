-- this CTE pairs the review, order and the customer so that it is posible to see what customers think of the product and how much said product was worth
with product_reviews as(
select
  ore.review_id,
  ore.order_id,
  c.customer_id,
  op.payment_value,
  ore.review_score,
  case
    When ore.review_score <= 2 then 'bad'
    when ore.review_score = 3 then 'okay'
    when ore.review_score <= 4 then 'great'
    else 'error'
  end as review_rating
from
  order_reviews as ore
  join orders as o on ore.order_id = o.order_id
  join customers as c on c.customer_id = o.customer_id
  join order_payments as op on op.order_id = o.order_id
  where review_rating = $1
group by
  review_id,
  ore.order_id,
  o.order_id,
  c.customer_id,
  o.customer_id,
  op.order_id,
  op.payment_value,
  ore.review_score
order by
  ore.review_score asc
  )
  select *
  from product_reviews
