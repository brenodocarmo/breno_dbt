
with order_status as (
    select
        d_orderstatus.sk_order_status
        ,s_orderstatus.id as order_id
        ,d_orderstatus.status


    from {{ ref('dim_order_status')}} as d_orderstatus
    left join {{ ref('stg_order_status')}} as s_orderstatus on s_orderstatus.order_status = d_orderstatus.status
    ),
    product as (
        select
            d_products.sk_produts
            ,s_orderitems.order_items_id
            ,d_products.product

        from {{ ref('dim_products')}} as d_products
        left join {{ ref('stg_order_items')}} as s_orderitems on s_orderitems.status = d_products.product

    ),
    sales_details as (
        select
            d_salesdetails.sk_sales_details
            ,s_orderitems.order_items_id
            ,d_salesdetails.year_manufacturer
            ,d_salesdetails.color
            ,d_salesdetails.status

        from {{ ref('dim_sales_details')}} as d_salesdetails
        left join {{ ref('stg_order_items')}} as s_orderitems 
            on s_orderitems.year_manufacturer = d_salesdetails.year_manufacturer
            and s_orderitems.color = d_salesdetails.color
            and s_orderitems.status = d_salesdetails.status

    ),
    services as (
        select
            d_services.sk_service
            ,s_orderitems.order_items_id
            ,d_services.service

        from {{ ref('dim_services')}} as d_services
        left join {{ ref('stg_order_items')}} as s_orderitems on s_orderitems.status = d_services.service

    ),
    vehicles as (
        select
            d_vehicles.sk_vehicles
            ,s_orderitems.order_items_id
            ,d_vehicles.manufacturer
            ,d_vehicles.model


        from {{ ref('dim_vehicles')}} as d_vehicles
        left join {{ ref('stg_order_items')}} as s_orderitems 
            on s_orderitems.manufacturer = d_vehicles.manufacturer
            and s_orderitems.model = d_vehicles.model

    ),

    dim_date as (
        select 
            date_day 
        from {{ ref('dim_tempo')}}
    ),
    source as (
        select
            s_orders.order_id
            ,s_orderitems.order_items_id
            ,s_employees.company_branches_id
            ,order_status.sk_order_status as fk_order_status
            ,s_orders.customer_id
            ,product.sk_produts as fk_produts
            ,sales_details.sk_sales_details as fk_sales_details
            ,services.sk_service as fk_service
            ,vehicles.sk_vehicles as fk_vehicles
            ,s_orders.employee_id
            ,s_orders.order_type_id
            ,s_employees.departament_id
            ,date(s_orders.order_date) as order_date
            ,time(s_orders.order_date) as order_time
            ,date(s_orders.return_date) as return_date
            ,time(s_orders.return_date) as return_time
            ,s_orders.days_diff
            ,s_orderitems.price

        from {{ ref('stg_order')}} as s_orders
        inner join {{ ref('stg_employees')}} as s_employees on s_employees.employee_id = s_orders.employee_id
        left join {{ ref('stg_order_items')}} as s_orderitems on s_orderitems.order_id = s_orders.order_id
        left join order_status on order_status.order_id = s_orders.order_id
        left join product on product.order_items_id = s_orderitems.order_items_id
        left join sales_details on sales_details.order_items_id = s_orderitems.order_items_id
        left join services on services.order_items_id = s_orderitems.order_items_id
        left join vehicles on vehicles.order_items_id = s_orderitems.order_items_id
        left join dim_date as dim_date_order  on dim_date_order.date_day = date(s_orders.order_date)
        LEFT JOIN dim_date as dim_date_return  on dim_date_return.date_day = date(s_orders.return_date)


        where s_orderitems.order_id is not null
    )

select * from source