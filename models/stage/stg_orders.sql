{{
  config(
    materialized = 'view',
    tags = ['staging', 'daily'],
    docs={
      'description': 'Transforms raw order data with currency conversion and status mapping',
      'columns': {
        'order_id': 'Primary order identifier',
        'amount_usd': 'Amount converted to USD using latest exchange rates',
        'status_code': 'Numeric representation of order status (1=completed, 2=processing, etc)'
      }
    }
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

transformed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        payment_method,
        country,
        {{ fiscal_quarter('order_date') }} AS order_fiscal_quarter,
        {{ currency_conversion(
            amount_column='COALESCE(amount, 0)',
            from_currency="CASE 
                WHEN country = 'US' THEN 'USD'
                WHEN country = 'UK' THEN 'GBP' 
                WHEN country = 'CA' THEN 'CAD'
                WHEN country = 'AU' THEN 'AUD'
                ELSE 'USD'
            END",
            to_currency='USD'
        ) }},
        CASE 
            WHEN status = 'completed' THEN 1
            WHEN status = 'processing' THEN 2
            WHEN status = 'returned' THEN 3
            WHEN status IS NULL THEN 0
            ELSE 4
        END AS status_code,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM source
)

SELECT * FROM transformed