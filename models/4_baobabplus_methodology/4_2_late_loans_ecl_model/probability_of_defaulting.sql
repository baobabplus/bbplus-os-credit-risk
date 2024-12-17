/*
    This transformation uses the output of the survival function to determine the immediate probability of defaulting of a late account
*/

WITH proba_reactivation as (
  SELECT * FROM {{ref('probability_calculation_survival')}}
),

repayment as (
  SELECT 
    *,
    1 - prob_cum as survival_func, -- opposite of probability of reactivation
    IF(days_since_cutoff = 270, 1 - prob_cum, null) as survival_func_at_PAR270 -- opposite of probability of reactivation at PAR270
  FROM proba_reactivation
),

get_survival_limit as (
  SELECT 
    perc_paid_bucket_start,
    perc_paid_bucket_end,
    MAX(survival_func_at_PAR270)  as survival_limit_at_PAR270,
    MIN(survival_func)            as survival_limit
  FROM repayment
  GROUP BY ALL
),

immediate_churn_and_repo_act as (
  SELECT 
    perc_paid_bucket_start,
    perc_paid_bucket_end,
    days_since_cutoff,
    survival_limit_at_PAR270,
    survival_limit,
    survival_func,
    -- probability of non-reactivation before PAR270 knowing there is non-reactivation event at time t
    SAFE_DIVIDE(IF(survival_limit_at_PAR270 is null, survival_limit, survival_limit_at_PAR270), survival_func) as immediate_prob_churn,
  FROM repayment
  LEFT JOIN get_survival_limit
    USING(perc_paid_bucket_start, perc_paid_bucket_end) 
),

immediate_churn as (
  SELECT 
    * EXCEPT(immediate_prob_churn),
    IF(days_since_cutoff > 270, 1, immediate_prob_churn) as immediate_prob_churn
  FROM immediate_churn_and_repo_act
)

SELECT * FROM immediate_churn
  