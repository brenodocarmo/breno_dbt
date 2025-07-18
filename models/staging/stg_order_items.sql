
with source as (
    select
        id
        ,order_id
        ,order_type_id
        ,split(upper(item), '@~|~@') as item
        
    from {{ source('sales_car', 'order_items')}}
    ),
    treated_sales as (
        select DISTINCT
            id as order_items_id
            ,order_id
            ,order_type_id
            ,item[OFFSET(0)] as manufacturer
            ,item[OFFSET(1)] as model
            ,safe_cast(item[OFFSET(2)] as INT64) as year_manufacturer
            ,case
                when item[OFFSET(3)] = 'SILVER' then 'PRATA'
                when item[OFFSET(3)] = 'BLACK'  then 'PRETO'
                when item[OFFSET(3)] = 'GREEN'  then 'VERDE'
                when item[OFFSET(3)] = 'WHITE'  then 'BRANCO'
                when item[OFFSET(3)] = 'BLUE'   then 'AZUL'
                when item[OFFSET(3)] = 'RED'    then 'VERMELHO'
                else 'OUTRO'
            end as color
            ,cast(0 as INT64) as status_id
            ,case
                when item[OFFSET(4)] = 'USED'     then 'USADO' 
                when item[OFFSET(4)] = 'NEW'      then 'NOVO' 
                when item[OFFSET(4)] = 'SEMI-NEW' then 'SEMI-NOVO' 
                else 'OUTRO'
            end as status
            ,safe_cast(item[OFFSET(5)] as NUMERIC) as price
        from source where order_type_id = 1
    ),
    treated_others_type as (
        select DISTINCT
            id as order_items_id
            ,order_id
            ,order_type_id
            ,(select distinct manufacturer from treated_sales where model = source.item[OFFSET(0)]) as manufacturer
            ,item[OFFSET(0)] as model
            ,cast(NULL as INT64) as year_manufacturer
            ,cast(NULL as STRING) as color
            ,cast(item[OFFSET(1)] as INT64) as status_id
            ,item[OFFSET(2)] as status
            ,safe_cast(item[OFFSET(3)] as NUMERIC) as price
        from source
        where order_type_id <> 1
    ),
    treated as (
        select * from treated_sales
        union all
        select * from treated_others_type
    )
select * from treated