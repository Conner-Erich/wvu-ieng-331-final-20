SELECT
  COUNT(*) AS total_rows,
  MIN(order_purchase_timestamp) AS min_date,
  MAX(order_purchase_timestamp) AS max_date,
  SUM(
    CASE
      WHEN order_purchase_timestamp > CURRENT_TIMESTAMP THEN 1
      ELSE 0
    END
  ) AS future_count
FROM
  orders
WHERE
  order_purchase_timestamp IS NOT NULL
