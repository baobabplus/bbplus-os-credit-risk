WITH accounts as (
    `steam-outlet-209412`.`oscreditrisk`.`raw_accounts`
)

SELECT 
  account_id, 
  CAST(registration_date AS TIMESTAMP) as registration_date, 
  CAST(unlock_price AS FLOAT) as unlock_price, 
  CAST(down_payment AS FLOAT) as down_payment, 
  CAST(down_payment_days_included AS FLOAT) as down_payment_days_included, 
  CAST(daily_rate AS FLOAT) as daily_rate, 
FROM accounts