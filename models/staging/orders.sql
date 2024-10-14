with
    source as (
        SELECT
            id as order_id
            ,safe_cast(customer_id AS INT64) AS customer_id
            , employee_id
            , order_type_id
            , SAFE_CAST(order_date AS DATETIME) order_date
            , SAFE_CAST(return_date AS DATETIME) AS return_date
            -- , TIME(SAFE_CAST(return_date AS TIMESTAMP)) AS return_hour
            , DATE_DIFF(SAFE_CAST(return_date AS DATETIME), SAFE_CAST(order_date AS DATETIME), DAY) AS days_diff 
        FROM {{source('sales_car','orders')}}
        WHERE customer_id IS NOT NULL -- we found two customer_id null's
  )


SELECT * FROM source