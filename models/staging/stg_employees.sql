

with source as (
    select
        id as employee_id
        ,departament_id
        ,case
            when departament = 'sales director'      then 'DIRETOR DE VENDAS'
            when departament = 'operations director' then 'DIRETOR DE OPERACOES'
            when departament = 'sales manager'       then 'GERENTE DE VENDAS'
            when departament = 'technical manager'   then 'GERENTE TECNICO'
            when departament = 'sales coordinator'   then 'COORDENADOR DE VENDAS'
            when departament = 'salesman'            then 'VENDEDOR'
            when departament = 'technical'           then 'TECNICO'
            when departament = 'mecanic'             then 'MECANICO'
            else 'OUTROS'
        end as departament
        ,company_branches_id
        ,upper(first_name) as first_name
        ,upper(last_name) as last_name
        ,gender
        ,upper(email) as email
    from  {{ source('seeds', 'employees')}}
)

select * from source