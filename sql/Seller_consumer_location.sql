-- This CTE finds what areas consume what products over time
WITH seller_consumer_location AS (
    SELECT
        s.seller_city,
        p.product_id,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month
    FROM sellers AS s
    FULL JOIN customers AS c  ON s.seller_city    = c.customer_city
    JOIN orders AS o          ON c.customer_id    = o.customer_id
    JOIN order_items AS oi    ON o.order_id       = oi.order_id
    JOIN products AS p        ON oi.product_id    = p.product_id
    WHERE s.seller_city = $1
    GROUP BY
        s.seller_city,
        p.product_id,
        DATE_TRUNC('month', o.order_purchase_timestamp)
    ORDER BY
        order_month,
        s.seller_city,
        p.product_id
)
SELECT *
FROM seller_consumer_location;
