
with source as (
    select distinct
        year_manufacturer
        ,color
        ,status
    from {{ ref('stg_order_items')}}
    where order_type_id = 1
)

select 
    row_number() over (order by year_manufacturer, color, status) as sk_sales_details
    ,year_manufacturer
    ,color
    ,status

from source