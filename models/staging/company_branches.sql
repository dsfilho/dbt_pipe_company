with
    source as (
        SELECT DISTINCT
            id as company_branches_id
           , country_code
           , upper(country) as country
           , state
           , upper(city) as city
           , CONCAT('LOJA - ', UPPER(REGEXP_REPLACE(name, r'^\d+ - ', ''))) AS name
           , latitude
           , longitude
           , phone
        FROM {{ source('sales_car','company_branches') }} 
    )

SELECT * FROM source