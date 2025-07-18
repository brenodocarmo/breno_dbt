
with source as (
select distinct
        order_status as status
    from {{ ref('stg_order_status')}}
    )

select
    row_number() over(order by status) as sk_order_status
    ,status
 from source