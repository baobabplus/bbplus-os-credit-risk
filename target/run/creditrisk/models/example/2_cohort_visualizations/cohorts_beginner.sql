

  create or replace view `steam-outlet-209412`.`oscreditrisk`.`cohorts_beginner`
  OPTIONS()
  as /*
    This transformations is aggregating on a cohort level for visualisation.
    It uses the beginner core dataset. 
    Produces information on raw payments only.
*/

WITH accounts_history as (
    SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`accounts_history_beginner`
),

filtered as (
    SELECT 
        * 
    FROM accounts_history
    -- optional : downsampling results to 1 point every 30 days. Often sufficient.
    WHERE  MOD(reporting_day, 30) = 1 
    -- removing the end of cohorts where calculation is not representative of the whole cohort
    QUALIFY reporting_day <= MIN(account_age_in_days) OVER(PARTITION BY cohort_month) 
)

-- Aggregating results on a cohort level
SELECT 
    cohort_month,
    reporting_day,
    SUM(paid_total) / SUM(unlock_price) as amount_paid_percent,
FROM filtered
GROUP BY ALL;

