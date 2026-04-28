SELECT
  COUNT(o.order_id) AS non_null_order_id_count,
  COUNT(o.customer_id) AS non_null_customer_id_count,
  COUNT(oi.product_id) AS non_null_product_id_count,
  COUNT(oi.seller_id) AS non_null_seller_id_count,
FROM
  order_items as oi
  join orders as o on o.order_id = oi.order_id
WHERE
  o.order_id
  or o.customer_id
  or oi.product_id
  or oi.seller_id IS NOT NULL
