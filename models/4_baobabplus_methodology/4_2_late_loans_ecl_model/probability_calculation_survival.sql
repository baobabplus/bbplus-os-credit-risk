/*
    This is a SQL implementation of the Kaplan Meier Survival Analysis.
    The output is a survival function. 
*/

WITH reactivation_history as (
    SELECT * FROM {{ref('history_disablements')}}
),

-- Split by perc paid bucket and take only last 3 years events observed
reactivation_history_filtered as (
    SELECT 
        *, 
        CASE 
            WHEN perc_paid <= 0.1 THEN '0'
            WHEN perc_paid <= 0.5 THEN '0.1'
            WHEN perc_paid <= 1 THEN '0.5'
            ELSE '0.5'
        END as perc_paid_bucket_start,
            CASE 
            WHEN perc_paid <= 0.1 THEN '0.1'
            WHEN perc_paid <= 0.5 THEN '0.5'
            WHEN perc_paid <= 1 THEN '1'
            ELSE '1'
        END as perc_paid_bucket_end
    FROM reactivation_history
),

cnt_subjects as (
  SELECT 
    perc_paid_bucket_start, 
    perc_paid_bucket_end, 
    COUNT(*) as num_subjects 
  FROM reactivation_history_filtered
  GROUP BY ALL 
),

daily as (
  SELECT
    perc_paid_bucket_start, 
    perc_paid_bucket_end,
    duration,
    COUNT(*) as num_obs,
    SUM(event) as num_events
  FROM reactivation_history_filtered
  GROUP BY ALL ORDER BY 1, 2, 3
),

at_risk_table as (
  SELECT
    perc_paid_bucket_start, 
    perc_paid_bucket_end,
    duration, 
    num_obs,
    num_events,
    num_subjects - COALESCE(SUM(num_obs) OVER (PARTITION BY perc_paid_bucket_start, perc_paid_bucket_end
      ORDER BY duration ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
     as at_risk
  FROM daily
  LEFT JOIN cnt_subjects USING(perc_paid_bucket_start, perc_paid_bucket_end)
),

survival_proba_table as (
  SELECT
    perc_paid_bucket_start, 
    perc_paid_bucket_end, 
    duration, 
    at_risk,
    num_obs,
    num_events,
    at_risk - num_events - COALESCE(LEAD(at_risk, 1) OVER (PARTITION BY perc_paid_bucket_start, perc_paid_bucket_end ORDER BY duration ASC), 0) as censored,
    EXP(SUM(SAFE.LN(1 - num_events / at_risk)) OVER (PARTITION BY perc_paid_bucket_start, perc_paid_bucket_end ORDER BY duration ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW)) as survival_proba
  FROM at_risk_table
),

get_density_proba as (
  SELECT  
    survival_proba_table.*,
    1 - survival_proba as prob_cum,
    (1 - survival_proba) - COALESCE(1 - LAG(survival_proba) OVER (PARTITION BY perc_paid_bucket_start, perc_paid_bucket_end ORDER BY duration ASC), 0) as prob_dens,
    ROW_NUMBER() OVER (PARTITION BY perc_paid_bucket_end ORDER BY duration ASC) as row_num
  FROM survival_proba_table
)

SELECT 
    * EXCEPT(duration, perc_paid_bucket_start, perc_paid_bucket_end),
    CAST(perc_paid_bucket_start AS FLOAT64) as perc_paid_bucket_start,
    CAST(perc_paid_bucket_end AS FLOAT64) as perc_paid_bucket_end,
    duration as days_since_cutoff 
FROM get_density_proba
ORDER BY perc_paid_bucket_start, duration


