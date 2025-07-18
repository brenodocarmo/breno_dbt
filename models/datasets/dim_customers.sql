
with source as (
    select
        customers_id
        ,first_name
        ,last_name
        ,gender
        ,job_title
        ,phone
        ,email
        ,country_code
        ,country
        ,city
        ,latitude
        ,longitude
        ,is_recurring
        ,level
        ,registration_date

    from {{ ref('stg_customers')}}
)

select * from source