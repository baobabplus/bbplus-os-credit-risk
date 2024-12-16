/*
    This transformations is just casting fields in the right format. 
    In practice, you may add here filtering steps to reduce your data to relevant records.
*/

WITH accounts as (
    SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`raw_accounts`
)

SELECT 
  account_id, 
  CAST(registration_date            AS TIMESTAMP)     as registration_date, 
  CAST(unlock_price                 AS FLOAT64)       as unlock_price, 
  CAST(down_payment                 AS FLOAT64)       as down_payment, 
  CAST(down_payment_days_included   AS FLOAT64)       as down_payment_days_included, 
  CAST(daily_rate                   AS FLOAT64)       as daily_rate, 
FROM accounts