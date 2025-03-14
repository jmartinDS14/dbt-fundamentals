-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.
select
  orderid,
  sum(amount) as total_amount
from {{ source('stripe', 'payment') }}
group by 1
having (total_amount < 0)