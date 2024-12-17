/*
    This transformations creates a dataset containing one line per account and contains the information if the account has defaulted or not. 
*/

WITH history as (
    SELECT * FROM {{ref('accounts_history_advanced')}}
    QUALIFY reporting_date = MAX(reporting_date) OVER()
),

detect_defaults as (
    SELECT 
        account_id,
        registration_date,
        perc_paid,
        CASE 
            WHEN reporting_date_status in ('UNLOCKED') THEN 0
            WHEN reporting_date_status in ('DETACHED', 'WRITTEN_OFF') THEN 1
            WHEN days_disabled >= 180 THEN 1
            ELSE 0
        END as has_defaulted,
    FROM history
)

SELECT * FROM detect_defaults
