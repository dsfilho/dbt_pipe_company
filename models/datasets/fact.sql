with 
    order_status as (
        SELECT
         dos.sk_order_status
        ,os.order_status_id as order_id
        ,dos.status
        FROM {{ref('dim_order_status')}} dos
        LEFT JOIN {{ref('order_status')}} os ON os.status = dos.status
 ),
 products as (
     SELECT  
         prd.sk_products
        ,oi.order_item_id
        ,prd.product
  FROM  {{ref('dim_products')}} prd
  LEFT JOIN {{ref('order_item')}} oi ON oi.status = prd.product
 ),
 sales_details as (
     SELECT  
         sde.sk_sales_details
        ,oi.order_item_id
        ,sde.year_manufacturer
        ,sde.color
        ,sde.status
  FROM  {{ref('dim_sales_details')}} sde
  LEFT JOIN {{ref('order_item')}} oi 
            ON  oi.year_manufacturer = sde.year_manufacturer
            AND oi.color =  sde.color
            AND oi.status = sde.status 
 ),
  services as (
     SELECT  
         srv.sk_services
        ,oi.order_item_id
        ,srv.service
  FROM  {{ref('dim_services')}} srv
  LEFT JOIN {{ref('order_item')}} oi ON oi.status = srv.service
 ),
  vehicles as (
     SELECT  
        veh.sk_vehicles
        ,oi.order_item_id
        ,veh.manufacturer
        ,veh.model
  FROM  {{ref('dim_vehicles')}} veh
  LEFT JOIN {{ref('order_item')}} oi 
            ON  oi.manufacturer = veh.manufacturer
            AND oi.model =  veh.model
           
 ),
    source as(
        SELECT 
            odr.order_id
            , ep.company_branches_id
            , oi.order_item_id
            , os.sk_order_status as fk_order_status
            , odr.customer_id
            , prd.sk_products as fk_products
            , sde.sk_sales_details as fk_sales_details
            , srv.sk_services as fk_services
            , veh.sk_vehicles as fk_vehicles
            , odr.employee_id
            , odr.order_type_id
            , ep.departament_id
            , EXTRACT(DATE FROM odr.order_date) as order_date 
            , EXTRACT(TIME FROM odr.order_date) as order_time 
            , EXTRACT(DATE FROM odr.return_date) as return_date
            , odr.days_diff
            , oi.price
        FROM {{ref('orders')}} odr
        INNER JOIN {{ref('stg_employees')}} ep ON ep.employee_id  = odr.employee_id
        LEFT JOIN {{ref ('order_item')}} oi ON oi.order_id = odr.order_id
        LEFT JOIN order_status os ON os.order_id = odr.order_id
        LEFT JOIN products prd ON prd.order_item_id = oi.order_item_id
        LEFT JOIN sales_details  sde ON sde.order_item_id = oi.order_item_id
        LEFT JOIN services  srv ON srv.order_item_id = oi.order_item_id
        LEFT JOIN vehicles  veh ON veh.order_item_id = oi.order_item_id
        WHERE oi.order_id IS NOT NULL AND prd.sk_products IS NOT NULL
    )

    select * FROM source