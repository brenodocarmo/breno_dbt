
with source as (
    select distinct
        case
            when model = 'FOCUS' then 'FORD'
            when model = 'W-RV' then 'HONDA'
            when model = 'AZERA' then 'HYUNDAI'
            else manufacturer
        end as manufacturer
        
        ,model
    from {{ ref('stg_order_items')}}
)

select
    row_number() over(order by manufacturer, model) as sk_vehicles
    ,manufacturer
    ,model
from source