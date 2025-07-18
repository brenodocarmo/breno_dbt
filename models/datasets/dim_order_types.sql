
with source as (
    select 
        order_types_id
        ,type
    from {{ ref('stg_order_types')}}
)

select * from source