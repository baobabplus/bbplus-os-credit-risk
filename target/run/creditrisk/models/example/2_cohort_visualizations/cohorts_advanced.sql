

  create or replace view `steam-outlet-209412`.`oscreditrisk`.`cohorts_advanced`
  OPTIONS()
  as /*
    This transformations is aggregating on a cohort level for visualisation.
    It uses the advanced core dataset.
    Produces information on linearized payments and with a term elapsed in % excluding downpayment.
*/

WITH accounts_history as (
    SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`accounts_history_advanced`
),

perc_elapsed as (
    SELECT 
        *,
        SAFE_DIVIDE(reporting_day_excl_dp, nominal_term_excl_dp) as perc_term_elapsed,
    FROM accounts_history
),

approximating as (
    SELECT
        *,
        -- Flooring the perc paid to the next 5% (we could choose another grain)
        FLOOR(perc_term_elapsed * 20) * 5 as perc_term_elapsed_approx,
    FROM perc_elapsed
),

downsampling as (
    SELECT 
        * 
    FROM approximating
    -- Optional : limit the time horizon to 200% of contractual term. Often sufficient.
    WHERE perc_term_elapsed_approx <= 200
    -- Ensure the dataset ends up with only one row per grain on % of contractual term
    QUALIFY ROW_NUMBER() OVER(PARTITION BY account_id, CAST(perc_term_elapsed_approx AS STRING) ORDER BY reporting_day_excl_dp) = 1
),

counting_accounts as (
  SELECT 
    *,
    COUNT(account_id) OVER (PARTITION BY cohort_month, CAST(perc_term_elapsed_approx AS INT64)) as cnt_accounts,
  FROM downsampling
),

filtering as (
    SELECT 
        *,
    FROM counting_accounts
    -- optional - removes the end of cohorts where calculation is not representative of the whole cohort. 
    -- We take 98% and not 100% as otherwise only a few outliers might prevent us from showing the cohort.
    QUALIFY cnt_accounts >= 0.98 * MAX(cnt_accounts) OVER(PARTITION BY cohort_month) 
),

aggregating as (
    SELECT 
        cohort_month,
        perc_term_elapsed_approx,
        SUM(paid_total_lin) / SUM(unlock_price - down_payment) as amount_paid_percent,
    FROM filtering
    GROUP BY ALL 
)

SELECT * FROM aggregating;

