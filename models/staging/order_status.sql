with
    source as (
        SELECT DISTINCT
            id AS order_status_id
           , CASE
                 WHEN order_status = 'delayed' THEN  'ATRASADO'
                 WHEN order_status = 'on hold' THEN  'EM ESPERA'
                 WHEN order_status = 'pending' THEN  'PRENDENTE'
                 WHEN order_status = 'shipped' THEN  'ENVIADO'
                 ELSE 'OUTRO'
            END as order_status
        --    , DATE_DIFF(SAFE_CAST(return_date AS TIMESTAMP), order_date, DAY) AS days_diff 
        FROM {{ source('sales_car', 'orders') }}
    )


SELECT * FROM source
