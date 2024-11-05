with
    source as (
        SELECT
            id
           , order_id
           , order_type_id
           , split(upper(item), '@~|~@') as item
           , ARRAY_LENGTH(REGEXP_EXTRACT_ALL(item, '@~\\|~@')) as occurrences
        FROM {{ source('sales_car','order_items')}}
        
    ),
    treated_sales as (
      SELECT DISTINCT
            id AS order_item_id
        , order_id
        , order_type_id
        , item[OFFSET(0)] AS manufacturer
        , item[OFFSET(1)] AS model
        , cast(item[OFFSET(2)] as INT64 ) AS year_manufacturer
        ,  CASE
                WHEN  item[OFFSET(3)]  = 'GREEN'  THEN 'VERDE'
                WHEN  item[OFFSET(3)]  = 'WHITE'  THEN 'BRANCO'
                WHEN  item[OFFSET(3)]  = 'RED'    THEN 'VERMELHO'
                WHEN  item[OFFSET(3)]  = 'SILVER' THEN 'PRATA'
                WHEN  item[OFFSET(3)]  = 'BLUE'   THEN 'AZUL'
                WHEN  item[OFFSET(3)]  = 'BLACK'  THEN 'PRETO'
                ELSE 'OUTRO'
            END  AS color
        , cast(0 as INT64) as status_id 
        , CASE
                WHEN  item[OFFSET(4)] = 'USED' THEN 'USADO'
                WHEN  item[OFFSET(4)] = 'NEW' THEN 'NOVO'    
                WHEN  item[OFFSET(4)] = 'SEMI-NEW' THEN 'SEMI-NOVO'
            END AS   status   
        , cast(item[OFFSET(5)] as numeric ) AS price

      FROM source where order_type_id = 1 
    ),
    treated_order_types as (
      SELECT DISTINCT
            id AS order_item_id
        , order_id
        , order_type_id
        , (select distinct manufacturer from treated_sales where model = source.item[OFFSET(0)]) AS manufacturer
        , item[OFFSET(0)] AS model
        , cast(NULL as int64) AS year_manufacturer
        , cast(NULL as String) AS color
        , cast(item[OFFSET(1)] AS INT64) AS status_id
        , item[OFFSET(2)] AS status
        , cast(item[OFFSET(3)] AS numeric ) AS price

      FROM source where order_type_id <> 1 
    ), treated as (
        select * from treated_sales union all select * from treated_order_types
    ), treated_nulos as (
        select * from treated where manufacturer is not null
    )

SELECT * FROM treated_nulos

