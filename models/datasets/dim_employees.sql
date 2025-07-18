
with source as (
    select distinct
        employee_id
        ,first_name
        ,last_name
        ,gender
        ,email

    from {{ ref('stg_employees')}}
)

select * from source