{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

WITH ranked as (
    SELECT 
    data:id::string as customer_id,
    data:first_name::string as first_name,
    data:last_name::string as last_name,
    data:email::string as email,
    data:created_at::timestamp as created_at,
    current_timestamp as load_timestamp,
    row_number() over (partition by data:id::string order by data:created_at desc) as rn
    FROM {{ source('raw','customers') }}
    {% if is_incremental() %}
        WHERE data:created_at > (SELECT max(created_at) from {{this}})
    {% endif %}
)

SELECT 
customer_id,
first_name,
last_name,
email,
created_at,
load_timestamp
FROM ranked
WHERE rn=1