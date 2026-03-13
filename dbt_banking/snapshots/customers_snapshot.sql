{% snapshot customers_snapshot %}

{{ 
    config(
        target_schema='ANALYTICS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='load_timestamp',
        invalidate_hard_deletes=True
    )
 }}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}