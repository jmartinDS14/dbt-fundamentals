{% set payment_method =['credit_card','coupon','bank_transfer','gift_card'] -%}

select 
    orderid,
    {%- for p in payment_method %}
        sum(case when paymentmethod = '{{p}}' then amount else 0 end) as {{p}}
        {%- if not loop.last -%}
            ,
        {%- endif -%}
    {% endfor %} 
from {{ source('stripe', 'payment') }}
group by 1