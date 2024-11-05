with    
    source  as (

        SELECT 
            DISTINCT
            manufacturer
           , model 
        FROM {{ ref('order_item') }}
       
    ) 

SELECT 
      row_number() over (order by manufacturer,model) as sk_vehicles
    , *
    FROM source