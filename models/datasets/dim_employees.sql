with 
    source as (

        SELECT DISTINCT
          employee_id
        , first_name
        , last_name
        , gender
        , email
        FROM  {{ ref('stg_employees') }}
    ) 


select * from source