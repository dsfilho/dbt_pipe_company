with
    source as ( 
        SELECT distinct
             status
        FROM {{ ref('order_status')  }}

    )

SELECT 
    row_number() over (order by status) as sk_order_status
    ,*
 FROM source