/*
    This is where the main calculation happens.
    Calculation of the PDs & PDs * EADs.
*/

{{ config(
    materialized='table'
)}}

WITH input as (
    SELECT * FROM {{ref('prepare_model_inputs')}}
),

calculated as (
  SELECT 
    perc_paid_current,
    account_segmentation,
    sample_end_date, 
    perc_paid_chunk_start,
    perc_paid_chunk_end,
    COUNT(*) as number_of_accounts,
    AVG(
      CASE 
        WHEN 
            has_defaulted = 1 AND 
            perc_paid >= perc_paid_chunk_start AND 
            perc_paid < perc_paid_chunk_end 
        THEN 1
        WHEN 
            has_defaulted = 0 
            AND perc_paid < perc_paid_chunk_start AND 
            -- This formula takes an assumption for future default rates of censored accounts.
            -- Here we consider the account will have 40% chances of defaulting in the remaining repayment
            rnd < 0.4 / ((1 - perc_paid_chunk_start) * 10) 
        THEN 1
        ELSE 0
      END
    ) as p_churn_incr, -- p_churn represents the probability of default
    AVG(
      CASE 
        WHEN 
            has_defaulted = 1 AND 
            perc_paid >= perc_paid_chunk_start AND 
            perc_paid < perc_paid_chunk_end 
        THEN 1 - perc_paid
        WHEN 
            has_defaulted = 0 AND 
            perc_paid < perc_paid_chunk_end AND 
            -- This formula takes an assumption for future default rates of censored accounts.
            -- Here we consider the account will have 40% chances of defaulting in the remaining repayment
            rnd < 0.4 / ((1 - perc_paid_chunk_start) * 10)
        THEN 1 - perc_paid_chunk_start
        ELSE 0
      END
    ) as r_churn_incr, -- r_churn represents the expected loss in receivable PD * EAD.
  FROM input
  GROUP BY ALL
)

SELECT 
    *,

    SUM(p_churn_incr) OVER(
        PARTITION BY CAST(perc_paid_current AS STRING), account_segmentation 
        ORDER BY perc_paid_chunk_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as p_churn,

    SUM(r_churn_incr) OVER(
        PARTITION BY CAST(perc_paid_current AS STRING), account_segmentation 
        ORDER BY perc_paid_chunk_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as r_churn, 

FROM calculated