{{
    config(
        materialized='table',
        unique_key='customer_key'
    )
}}

with snapshot_data as(
    SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id','dbt_updated_at']) }} as customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    dbt_valid_from as start_date,
    dbt_valid_to as end_date,
    CASE WHEN dbt_valid_to is NULL THEN TRUE ELSE FALSE END as is_current
    FROM {{ ref('customers_snapshot') }}
)

SELECT * FROM snapshot_data