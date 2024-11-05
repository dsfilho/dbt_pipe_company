with    
    source  as (
        SELECT DISTINCT
             year_manufacturer
            , color
            , status
        FROM {{ ref('order_item') }}
        WHERE order_type_id = 1
    ) 

SELECT 
    row_number() over (order by year_manufacturer, color, status) as sk_sales_details
    , * 
 FROM source