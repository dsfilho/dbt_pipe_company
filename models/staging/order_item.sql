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
    treated as (
      SELECT DISTINCT
            id AS order_item_id
        , order_id
        , order_type_id
        , item[OFFSET(0)] AS manufacturer
        , item[OFFSET(1)] AS model
        , item[OFFSET(2)] AS year_manufacturer
        ,  CASE
                WHEN  item[OFFSET(3)]  = 'GREEN'  THEN 'VERDE'
                WHEN  item[OFFSET(3)]  = 'WHITE'  THEN 'BRANCO'
                WHEN  item[OFFSET(3)]  = 'RED'    THEN 'VERMELHO'
                WHEN  item[OFFSET(3)]  = 'SILVER' THEN 'PRATA'
                WHEN  item[OFFSET(3)]  = 'BLUE'   THEN 'AZUL'
                WHEN  item[OFFSET(3)]  = 'BLACK'  THEN 'PRETO'
                ELSE 'OUTRO'
            END  AS color
        ,  CASE
                WHEN  item[OFFSET(4)] = 'USED' THEN 'USADO'
                WHEN  item[OFFSET(4)] = 'NEW' THEN 'NOVO'    
                WHEN  item[OFFSET(4)] = 'SEMI-NEW' THEN 'SEMI-NOVO'
            END AS   status   
        , item[OFFSET(5)] AS price

      FROM source where occurrences = 5
    )


SELECT * FROM treated

