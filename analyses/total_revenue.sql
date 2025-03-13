select 
    sum(amount) 
from {{ ref('stg_stripe__payments') }}
where status = 'success'