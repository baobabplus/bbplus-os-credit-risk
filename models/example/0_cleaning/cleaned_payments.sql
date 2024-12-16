/*
    This transformations is just casting fields in the right format. 
    In practice, you may add here filtering steps to reduce your data to relevant records.
*/

WITH payments as (
    SELECT * FROM {{ref('raw_payments')}}
)

SELECT 
  account_id,
  CAST(down_payment             AS BOOLEAN)         as down_payment,
  CAST(payment_effective_date   AS TIMESTAMP)       as payment_effective_date,
  CAST(amount                   AS FLOAT64)         as amount,
FROM payments