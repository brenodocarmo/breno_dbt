
with source as (
    select distinct
        departament_id
        ,departament
    from {{ ref('stg_employees')}}
)

select * from source