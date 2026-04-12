{% snapshot scd_customers %}

{{
    config(
        target_schema='public',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='converted_updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    name,
    email,
    city,
    created_at,
    updated_at,
    TO_TIMESTAMP(updated_at / 1000000.0) AS converted_updated_at
FROM {{ source('cdc', 'customers') }}

{% endsnapshot %}
