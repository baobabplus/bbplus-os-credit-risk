{{ config(
    materialized='table',
    partition_by={
        "field": "reporting_date",
        "data_type" : "timestamp",
        "granularity": "day"
    },
    cluster_by = 'account_id'
)}}

WITH accounts_history as (
    SELECT * FROM {{ref('accounts_history_advanced')}}
),

repo as (
    SELECT * FROM {{ref('repossession_valuation_parameters')}}
),

proba_churn_repayment as (
    SELECT * FROM {{ref('prepare_model_outputs')}}
),

proba_churn_immediate as (
    SELECT * FROM {{ref('probability_of_defaulting')}}
),

batch_to_compute as (
    SELECT
        account_id, 
        reporting_date,
        reporting_day,
        portfolio_scope,
        days_disabled,
        perc_paid,
        IF(perc_paid < 0.1, '0. At registration', account_segmentation) as account_segmentation, 
    FROM accounts_history
    WHERE portfolio_scope
),

join_with_churn_model as (
    SELECT     
        account_id,
        reporting_date,
        SUM(proba_churn_repayment.p_churn_incr)        as p_churn,
        SUM(proba_churn_repayment.r_churn_incr)        as r_churn,
    FROM batch_to_compute
    LEFT JOIN proba_churn_repayment
    ON 
        batch_to_compute.account_segmentation = proba_churn_repayment.account_segmentation AND
        batch_to_compute.perc_paid < proba_churn_repayment.perc_paid_chunk_end
    GROUP BY ALL
),

join_with_immediate_churn_model as (
    SELECT 
        account_id, 
        reporting_date,
        COALESCE(proba_churn_immediate.immediate_prob_churn, 0) as immediate_prob_churn,
        COALESCE(proba_churn_immediate.immediate_prob_churn, 0) * (1 - perc_paid) as immediate_loss,
    FROM batch_to_compute
    LEFT JOIN proba_churn_immediate
    ON  
        batch_to_compute.perc_paid >= proba_churn_immediate.perc_paid_bucket_start AND 
        batch_to_compute.perc_paid < proba_churn_immediate.perc_paid_bucket_end AND 
        batch_to_compute.days_disabled = proba_churn_immediate.days_since_cutoff
),

joint_together as (
    SELECT * FROM batch_to_compute
    LEFT JOIN join_with_churn_model USING(account_id, reporting_date)
    LEFT JOIN join_with_immediate_churn_model USING(account_id, reporting_date)
    CROSS JOIN repo
),

combine_outputs_1 as (
    SELECT 
        account_id,
        reporting_date,
        reporting_day, 
        days_disabled,
        perc_paid,
        account_segmentation,

        p_churn as repayment_prob_churn,
        r_churn as repayment_loss,
        p_churn * probability_of_repossession * repossession_value as repayment_recoveries,
        
        immediate_prob_churn,
        immediate_loss,
        immediate_prob_churn * probability_of_repossession * repossession_value as immediate_recoveries,

    FROM joint_together
),

combine_outputs_2 as (
    SELECT 
        *,
        IF(immediate_loss - immediate_recoveries < 0, 0, immediate_loss - immediate_recoveries) as immediate_ecl,
        IF(repayment_loss - repayment_recoveries < 0, 0, repayment_loss - repayment_recoveries) as repayment_ecl,
    FROM combine_outputs_1
),

combine_outputs_3 as (
    SELECT 
        *,
        immediate_loss + (1 - immediate_prob_churn) * repayment_loss as total_loss,
        immediate_prob_churn + (1 - immediate_prob_churn) * repayment_prob_churn as total_pchurn,
        immediate_ecl + (1 - immediate_prob_churn) * repayment_ecl as total_ecl,
    FROM combine_outputs_2
)

SELECT * FROM combine_outputs_3