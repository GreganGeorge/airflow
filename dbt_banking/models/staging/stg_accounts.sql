{{ config(
    materialized='view'
) }}

WITH ranked as (
    SELECT 
    data:id::string as account_id,
    data:customer_id::string as customer_id,
    data:account_type::string as account_type,
    data:balance::string as balance,
    data:currency::string as currency,
    data:created_at::timestamp as created_at,
    current_timestamp as load_timestamp,
    row_number() over (partition by data:id::string order by data:created_at desc) as rn
    FROM {{ source('raw','accounts') }}
)

SELECT 
account_id,
customer_id,
account_type,
balance,
currency,
created_at,
load_timestamp
FROM ranked
WHERE rn=1