{{
  config(
    materialized = 'table',
    tags = ['marts', 'monthly'],
    partition_by = {
      'field': 'first_order_fiscal_quarter',
      'data_type': 'string'
    },
    cluster_by = ['customer_tier']
  )
}}

WITH customer_orders AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

customer_segments AS (
    SELECT
        co.*,
        CASE
            WHEN total_spent_usd >= 1000 THEN 'High Value'
            WHEN total_spent_usd >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment,
        CASE
            WHEN number_of_orders >= 5 THEN 'Frequent'
            WHEN number_of_orders >= 2 THEN 'Occasional'
            ELSE 'One-time'
        END AS frequency_segment,
        CASE
            WHEN months_since_first_order <= 3 THEN 'New'
            WHEN months_since_first_order <= 12 THEN 'Established'
            ELSE 'Long-term'
        END AS tenure_segment,
        total_spent_usd / NULLIF(months_since_first_order, 0) AS monthly_spending_rate,
        {{ fiscal_quarter("first_order_date") }} AS first_order_fiscal_quarter,
        {{ fiscal_quarter("most_recent_order_date") }} AS recent_order_fiscal_quarter
    FROM customer_orders co
),

final AS (
    SELECT
        cs.*,
        monthly_spending_rate * 12 AS projected_annual_value,
        monthly_spending_rate * 24 AS projected_two_year_value,
        RANK() OVER (PARTITION BY customer_tier ORDER BY total_spent_usd DESC) AS tier_rank,
        PERCENT_RANK() OVER (ORDER BY total_spent_usd) AS percentile_rank,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM customer_segments cs
)

SELECT * FROM final