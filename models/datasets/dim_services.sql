with    
    source  as (

        SELECT 
            DISTINCT
            status as service
        FROM {{ ref('order_item') }}
        WHERE order_type_id IN (2,3)
    ) 

SELECT 
      row_number() over (order by service) as sk_services
    , *
    FROM source