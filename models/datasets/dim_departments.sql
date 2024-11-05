with 
    source as (

        SELECT DISTINCT
         departament_id
         ,departament
        FROM  {{ ref('stg_employees') }}
    ) 


select * from source