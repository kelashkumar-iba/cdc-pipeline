SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS total_spent,
    MAX(o.created_at) AS last_order_at
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.city
