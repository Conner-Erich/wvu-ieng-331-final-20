--this CTE finds what areas consume what products
with seller_comsumer_location as (
select
  s.seller_city,
  p.product_id
from
  sellers as s
  full join customers as c on s.seller_city = c.customer_city
  JOIN orders AS o ON c.customer_id = o.customer_id
  JOIN order_items AS oi ON o.order_id = oi.order_id
  JOIN products AS p ON oi.product_id = p.product_id
  where s.seller_city = $1
group by
  s.seller_city,
  oi.product_id,
  p.product_id
ORDER BY
  s.seller_city,
  p.product_id
)
select *
from seller_comsumer_location
