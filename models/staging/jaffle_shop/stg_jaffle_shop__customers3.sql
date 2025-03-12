with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

transformed as (

    select 
        first_name as customer_first_name,
        last_name as customer_last_name,
        id as customer_id
    from source

)

select * from transformed