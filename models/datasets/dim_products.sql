with    
    source  as (

        SELECT 
            DISTINCT
            status as product
        FROM {{ ref('order_item') }}
        WHERE order_type_id = 4
    ) 

SELECT 
      row_number() over (order by product) as sk_products
    , *
    FROM source