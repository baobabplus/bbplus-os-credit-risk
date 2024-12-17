/*
    This transformations generates a table representing the segmentation A, B, C and D
    for each account depending on the progress on repayment. 
    It will be used to train the model to pick up these segmentations.
*/

WITH accounts_history as (
    SELECT * FROM {{ref('accounts_history_advanced')}}
),

segmentation_at_0 as (
  SELECT 
    account_id,
    0 as perc_paid_current,
    '0. At registration' as account_segmentation,
  FROM accounts_history
  WHERE reporting_day = 1
),

segmentation_at_10 as (
  SELECT 
    account_id,
    0.1 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_20 as (
  SELECT 
    account_id,
    0.2 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_30 as (
  SELECT 
    account_id,
    0.3 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.3
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_40 as (
  SELECT 
    account_id,
    0.4 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.4
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_50 as (
  SELECT 
    account_id,
    0.5 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.5
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_60 as (
  SELECT 
    account_id,
    0.6 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.6
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_70 as (
  SELECT 
    account_id,
    0.7 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.7
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_80 as (
  SELECT 
    account_id,
    0.8 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.8
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
),

segmentation_at_90 as (
  SELECT 
    account_id,
    0.9 as perc_paid_current,
    account_segmentation,
  FROM accounts_history
  WHERE perc_paid >= 0.9
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY reporting_day) = 1
)

SELECT * FROM segmentation_at_0
UNION ALL
SELECT * FROM segmentation_at_10
UNION ALL
SELECT * FROM segmentation_at_20
UNION ALL
SELECT * FROM segmentation_at_30
UNION ALL
SELECT * FROM segmentation_at_40
UNION ALL
SELECT * FROM segmentation_at_50
UNION ALL
SELECT * FROM segmentation_at_60
UNION ALL
SELECT * FROM segmentation_at_70
UNION ALL
SELECT * FROM segmentation_at_80
UNION ALL
SELECT * FROM segmentation_at_90