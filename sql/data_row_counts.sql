SELECT
COUNT(*) AS row_count
FROM orders
UNION ALL
SELECT
    COUNT(*) AS row_count
FROM order_items
UNION ALL
SELECT
    COUNT(*) AS row_count
FROM customers
ORDER BY row_count DESC
