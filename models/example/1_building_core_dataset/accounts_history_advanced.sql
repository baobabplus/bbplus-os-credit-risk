/*
    This transformations is generating the second version of the core dataset (advanced version)
    The main complexity lies in the fact that SQL is not sufficient to calculate the linearized payments. 
    We thus need to use a User Defined Function (here in Javascript) - but we could also use another programming language. 
*/

{{ config(
    materialized='table',
    partition_by={
        "field": "reporting_date",
        "data_type" : "timestamp",
        "granularity": "day"
    },
    cluster_by = ['account_id']
) }}

WITH accounts_history as (
    SELECT * FROM {{ref('accounts_history_beginner')}}
),

additional_kpis as (
    SELECT 
        *,
        
        CASE 
            WHEN reporting_day <= down_payment_days_included THEN Null
            WHEN reporting_day = down_payment_days_included + 1 THEN paid_total
            ELSE amount_excl_dp
        END as amount_excl_dp_period, -- This step is necessary to 'record' payments at the end of the downpayment period.

        CASE  
            WHEN reporting_day < down_payment_days_included THEN Null
            WHEN reporting_day >= down_payment_days_included THEN reporting_day - down_payment_days_included
        END as reporting_day_excl_dp, -- Necessary to remove the downpayment period from analyses

        GREATEST(
            0,
            MAX(reporting_day - down_payment_days_included) OVER(PARTITION BY account_id)
         ) as account_age_excl_dp_in_days,

    FROM accounts_history
),

-- Preparing the data for the UDF (consuming arrays)
prepared_for_udf as (
    SELECT
        account_id,
        ARRAY_AGG(COALESCE(amount_excl_dp_period, 0)     ORDER BY reporting_day) as payment_amounts,
        ARRAY_AGG(daily_rate                             ORDER BY reporting_day) as daily_rates,
        ARRAY_AGG(CAST(DATE(reporting_date) as STRING)   ORDER BY reporting_day) as casted_reporting_dates,
    FROM additional_kpis
    GROUP BY ALL
),

-- applying the UDF on prepared data format
apply_udf AS (
    SELECT 
        *,
        {{target.schema}}.payment_linearization(
        prepared_for_udf.payment_amounts, 
        prepared_for_udf.daily_rates, 
        prepared_for_udf.casted_reporting_dates
        ) as payment_amount_lin_excl_dp
    FROM prepared_for_udf
),

-- Expanding the results before joining them back
expand_udf_result AS (
    SELECT
        account_id,
        CAST(reporting_date AS TIMESTAMP) as reporting_date,
        amount_lin
    FROM
        apply_udf,
        UNNEST(apply_udf.casted_reporting_dates)            AS reporting_date   WITH OFFSET AS pos
    JOIN UNNEST(apply_udf.payment_amount_lin_excl_dp)     AS amount_lin       WITH OFFSET AS val_pos
    ON pos = val_pos
),

join_back_on_dataset as (
    SELECT 
        *,
        SUM(amount_lin) OVER(PARTITION BY account_id ORDER BY reporting_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as paid_total_lin,
        IF(
            amount_lin = 0 AND LAG(amount_lin) OVER(PARTITION BY account_id ORDER BY reporting_day) > 0,
            reporting_date,
            Null
        ) as last_disablement,
    FROM additional_kpis 
    LEFT JOIN expand_udf_result USING(account_id, reporting_date)
),

-- As a last step, use this information to calculate useful fields: status and number of days disabled.
final_kpis as (
  SELECT 
    *,
    CASE 
        WHEN reporting_day <= down_payment_days_included THEN 'ENABLED'
        WHEN paid_total >= unlock_price THEN 'UNLOCKED'
        WHEN amount_lin > 0   THEN 'ENABLED'
        WHEN amount_lin <= 0  THEN 'DISABLED'
    END as reporting_date_status,
    CASE 
        WHEN amount_lin = 0 
        THEN DATE_DIFF(
            reporting_date, 
            LAST_VALUE(last_disablement IGNORE NULLS) OVER(PARTITION BY account_id ORDER BY reporting_day),
            DAY
        ) 
    END as days_disabled,
  FROM join_back_on_dataset
)

SELECT * FROM final_kpis
