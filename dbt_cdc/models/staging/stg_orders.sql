SELECT
    order_id,
    customer_id,
    product,
    amount,
    status,
    created_at,
    updated_at
FROM {{ source('cdc', 'orders') }}
