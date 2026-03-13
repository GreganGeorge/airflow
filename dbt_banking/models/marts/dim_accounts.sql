{{
    config(
        materialized='table',
        unique_key='account_key'
    )
}}

with snapshot_data as(
    SELECT 
    {{ dbt_utils.generate_surrogate_key(['account_id','dbt_updated_at']) }} as account_key,
    customer_id,
    account_type,
    balance,
    currency,
    dbt_valid_from as start_date,
    dbt_valid_to as end_date,
    CASE WHEN dbt_valid_to is NULL THEN TRUE ELSE FALSE END as is_current
    FROM {{ ref('accounts_snapshot') }}
)

SELECT * FROM snapshot_data