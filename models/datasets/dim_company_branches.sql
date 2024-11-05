with 
    source as (

        SELECT DISTINCT
             company_branches_id
           , country_code
           , upper(country) as country
           , state
           , city
           , name
           , latitude
           , longitude
           , phone
        FROM  {{ ref('company_branches') }}
    ) 


select * from source