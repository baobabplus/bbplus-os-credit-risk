/*
    This transformation prepares a dataset in the right format for a survival analysis.
    The target dataset contains one line per disablement period. 
    Duration represents the duration of the period. Event represents the fact that there was a payment interrupting the period. 
*/

WITH accounts_history as (
    SELECT * FROM {{ref('accounts_history_advanced')}}
),

get_disablement_periods as (
    SELECT 
        account_id,
        reporting_date,
        perc_paid,
        LAST_VALUE(last_disablement IGNORE NULLS) OVER(PARTITION BY account_id ORDER BY reporting_day) as last_disablement,
        days_disabled,
    FROM accounts_history
),

aggregated_disablement_periods as (
    SELECT 
        account_id,
        reporting_date,
        perc_paid,
        days_disabled as duration,
    FROM get_disablement_periods
    QUALIFY days_disabled = MAX(days_disabled) OVER(PARTITION BY account_id, last_disablement)
)


SELECT 
    *,
    IF(reporting_date = MAX(reporting_date) OVER(), 0, 1) as event,
FROM aggregated_disablement_periods