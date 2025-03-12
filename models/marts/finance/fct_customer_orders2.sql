with

    -- Mart CTEs
    paid_orders as (select * from {{ ref("int_paid_order") }}),

    -- final CTEs
    final as (
        select
            p.*,
            row_number() over (
                order by p.order_placed_at, p.order_id
            ) as transaction_seq,
            row_number() over (
                partition by p.customer_id order by p.order_placed_at, p.order_id
            ) as customer_sales_seq,
            case
                when
                    (
                        rank() over (
                            partition by p.customer_id
                            order by p.order_placed_at, p.order_id
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as nvsr,
            sum(p.total_amount_paid) over (
                partition by p.customer_id order by p.order_placed_at, p.order_id
            ) as customer_lifetime_value,
            first_value(p.order_placed_at) over (
                partition by p.customer_id
                order by p.order_placed_at, p.order_id
            ) as fdos

        from paid_orders p
    )

select *
from final
order by order_id
