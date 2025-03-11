with

    customers as (select * from {{ ref('stg_jaffle_shop__customers2') }}),

    orders as (select * from {{ ref('int_orders') }}),

    customer_orders as (
        select
            orders.*,
            customers.full_name,
            customers.surname,
            customers.givenname,

            min(order_date) over(partition by orders.customer_id) as first_order_date,

            min(valid_order_date) over(partition by orders.customer_id) as first_non_returned_order_date,

            max(valid_order_date) over(partition by orders.customer_id) as most_recent_non_returned_order_date,

            count(*) over(partition by orders.customer_id) as order_count,

            sum(nvl2(orders.valid_order_date,1,0)) over(partition by orders.customer_id) as non_returned_order_count,

            sum(nvl2(orders.valid_order_date,order_value_dollars,0)) over(partition by orders.customer_id) as total_lifetime_value,

            sum(nvl2(orders.valid_order_date,order_value_dollars,0)) over(partition by orders.customer_id)
            / sum(nvl2(orders.valid_order_date,1,0)) over(partition by orders.customer_id) as avg_non_returned_order_value,

            array_agg(distinct orders.order_id) over(partition by orders.customer_id) as order_ids
        from orders
        inner join customers
            on orders.customer_id = customers.customer_id
    ),

    -- Final CTEs 
    final as (

        select

            orders.order_id as order_id,
            orders.customer_id as customer_id,
            customers.surname,
            customers.givenname,
            customer_orders.first_order_date,
            customer_orders.order_count,
            customer_orders.total_lifetime_value,
            orders.order_value_dollars,
            orders.order_status as order_status,
            orders.payment_status

        from orders

        join customers on orders.customer_id = customers.customer_id

        join
            customer_orders
            on orders.customer_id = customer_orders.customer_id



    )

-- Simple Select Statement
select *
from final
