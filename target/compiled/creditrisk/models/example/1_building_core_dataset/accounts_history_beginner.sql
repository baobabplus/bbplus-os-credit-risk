/*
    This transformations is generating the first version of the core dataset (beginner version). 
    It is essentially : 
    - Joining the accounts dataset with a date spine for the target granuarity
    - Grouping the payment by day, and joining them to the accounts history
    - Calculating cumulated sums to describe the cumulated amount paid, and a few other useful fields
*/



WITH accounts as (
  SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`cleaned_accounts`
),

payments as (
  SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`cleaned_payments`
),

date_spine as (
  SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`date_spine`
),

accounts_with_spine as (
  SELECT 
    *,
    TIMESTAMP_DIFF(reporting_date, registration_date, DAY) + 1 as reporting_day,
  FROM accounts
  LEFT JOIN date_spine
  ON accounts.registration_date <= date_spine.reporting_date
),

payments_grouped_by_day as (
  SELECT 
    account_id,
    
    DATE_ADD(
      DATE_TRUNC(payment_effective_date, DAY), 
      INTERVAL 1 DAY
    ) as reporting_date,

    SUM(amount) as amount,
    SUM(
      IF(not down_payment, amount, 0)
    ) as amount_excl_dp,

  FROM payments
  GROUP BY ALL
),

joint as (
  SELECT 
    * EXCEPT(amount, amount_excl_dp),
    COALESCE(amount,          0) as amount,
    COALESCE(amount_excl_dp,  0) as amount_excl_dp,
  FROM accounts_with_spine 
  LEFT JOIN payments_grouped_by_day 
  USING(account_id, reporting_date)
),

calc_paid_total as (
  SELECT 
    *,
    SUM(amount)         OVER(PARTITION BY account_id ORDER BY reporting_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as paid_total,
    SUM(amount_excl_dp) OVER(PARTITION BY account_id ORDER BY reporting_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as paid_total_excl_dp,
    
    SAFE_DIVIDE(unlock_price - down_payment, daily_rate) + down_payment_days_included as nominal_term,
    SAFE_DIVIDE(unlock_price - down_payment, daily_rate)                              as nominal_term_excl_dp,

    MAX(reporting_day) OVER(PARTITION BY account_id) as account_age_in_days,
    GREATEST(
            0,
            MAX(reporting_day - down_payment_days_included) OVER(PARTITION BY account_id)
    ) as account_age_excl_dp_in_days,

    DATE_TRUNC(registration_date, MONTH) as cohort_month,
    DATE_TRUNC(registration_date, QUARTER) as cohort_quarter,
    DATE_TRUNC(registration_date, YEAR) as cohort_year,

  FROM joint
)

SELECT * FROM calc_paid_total