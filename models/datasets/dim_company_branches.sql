
with source as (
    select distinct
        company_branches_id
        ,country_code
        ,country
        ,state
        ,city 
        ,name
        ,latitude 
        ,longitude
        ,phone 
    from {{ ref('stg_company_branches')}}
)

select * from source