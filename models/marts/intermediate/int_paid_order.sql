with
    payments as (select * from {{ ref('stg_stripe__payments3') }}),
    orders as (select * from {{ ref('stg_jaffle_shop__orders2') }}),
    customers as (select * from {{ ref('stg_jaffle_shop__customers3') }}),

    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.customer_first_name,
            c.customer_last_name
        from orders as orders
        left join payments p
            on orders.order_id = p.order_id
        left join customers c on orders.customer_id = c.customer_id
    )

select * from paid_orders