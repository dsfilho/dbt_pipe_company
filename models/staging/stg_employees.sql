{{ config(alias='employees')}}
with
    source as (
        SELECT 
          id as employee_id
        , departament_id
        , CASE
            WHEN  departament = 'sales manager' then 'GERENTE DE VENDA'
            WHEN  departament = 'sales coordinator' then 'COORDENADOR DE VENDA'
            WHEN  departament = 'salesman' then 'VENDEDOR'
            WHEN  departament = 'operations director' then 'DIRETOR DE OPERAÇÕES'
            WHEN  departament = 'technical manager' then 'GERENTE TÉCNICO'
            WHEN  departament = 'technical' then 'TÉCNICO'
            WHEN  departament = 'mecanic' then 'MECÂNICO'
            WHEN  departament = 'sales director' then 'DIRETOR DE VENDAS'
            ELSE 'OUTRO'
          END AS departament
        , company_branches_id
        , UPPER(first_name) as first_name
        , UPPER(last_name)  as last_name
        , gender
        , UPPER(email) as email
        FROM {{source('seeds','employees')}}
    )


SELECT * FROM source