with 
    source as (

        SELECT DISTINCT
         order_types_id
         ,type
        FROM  {{ ref('order_types') }}
    ) 


select * from source