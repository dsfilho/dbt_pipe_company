{{ config(alias='customers')}}

with source as (

        SELECT
            id as customer_id
           , UPPER(first_name) as first_name
           , UPPER(last_name) as last_name
           , gender 
           , UPPER(job_title) as job_title
           , phone
           , UPPER(email) as email
           , country_code 
           , UPPER(country) as country
           , UPPER(city) as city
           , latitude
           , longitude
           , recurring_customer as is_recurring
           , UPPER(customer_level) as level
           , registration_date

        FROM {{source('seeds', 'customers')}}
)


SELECT * FROM source