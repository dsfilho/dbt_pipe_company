with
    source as (
        SELECT 
            id  AS  order_types_id
            , CASE
                WHEN type = 'sales' then 'VENDAS'
                WHEN type = 'tecnical_review' THEN 'REVISÃO TÉCNICA'
                WHEN type = 'preventive_maintenance' THEN 'MANUTENÇÃO PREVENTIVA'
                WHEN type = 'body_shop' THEN 'LOJA'
                ELSE 'OUTROS'
              END AS TYPE
        FROM {{ source('sales_car','order_types') }}
    )

SELECT * FROM source