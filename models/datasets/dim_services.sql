
with source as (
    select distinct
        status as service

    from {{ ref('stg_order_items')}}
    where order_type_id in (2, 3)
)

select
    row_number() over(order by service) as sk_service
    ,service
from source