SELECT
    customer_id,
    name,
    email,
    city,
    created_at,
    updated_at
FROM {{ source('cdc', 'customers') }}
