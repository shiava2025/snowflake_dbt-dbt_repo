{{
  config(
    materialized = 'view',
    tags = ['staging', 'daily']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
),

transformed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        date_of_birth,
        {{ calculate_age('date_of_birth') }} AS customer_age,
        first_order_date,
        customer_tier,
        {{ fiscal_quarter('first_order_date') }} AS first_order_fiscal_quarter,
        CASE 
            WHEN customer_tier = 'Gold' THEN 1
            WHEN customer_tier = 'Silver' THEN 2
            WHEN customer_tier = 'Bronze' THEN 3
            ELSE 4
        END AS customer_tier_rank,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM source
)

SELECT * FROM transformed