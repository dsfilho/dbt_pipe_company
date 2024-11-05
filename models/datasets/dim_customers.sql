with 
    source as (

        SELECT DISTINCT
       
             customer_id
           , first_name
           , last_name
           , gender 
           , job_title
           , phone
           , email
           , country_code 
           , country
           , city
           , latitude
           , longitude
           , is_recurring
           , level
           , registration_date
       
        FROM  {{ ref('stg_customers') }}
    ) 


select * from source