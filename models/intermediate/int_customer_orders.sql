{{
  config(
    materialized = 'table',
    tags = ['intermediate', 'monthly']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.customer_tier,
        c.customer_age,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS most_recent_order_date,
        COUNT(o.order_id) AS number_of_orders,
        SUM(CASE WHEN o.status = 'completed' THEN o.amount ELSE 0 END) AS total_spent,
        SUM(CASE WHEN o.status = 'completed' THEN o.amount_usd ELSE 0 END) AS total_spent_usd,
        AVG(CASE WHEN o.status = 'completed' THEN o.amount ELSE NULL END) AS average_order_value,
        MAX(CASE WHEN o.status = 'completed' THEN o.amount ELSE NULL END) AS largest_order_value,
        {{ calculate_age("MIN(o.order_date)", 'month') }} AS months_since_first_order
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    {% if target.name == 'dev' %}
    WHERE c.customer_tier = 'Gold'
    {% endif %}
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_tier, c.customer_age
)

SELECT * FROM customer_orders