
with source as (
    select distinct
        status as product
    from {{ ref('stg_order_items')}}

    WHERE order_type_id = 4
)

select
    row_number() over (order by product) as sk_produts
    ,product
from source