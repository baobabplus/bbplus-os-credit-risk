WITH accounts as (
    SELECT * FROM `steam-outlet-209412`.`oscreditrisk`.`cleaned_accounts`
),

min_max_dates as (
    SELECT 
        CAST(MIN(registration_date) AS DATE) as min_date,
        CAST(MAX(registration_date) AS DATE) as max_date,
    FROM accounts
)

SELECT
    TIMESTAMP_ADD(
        (SELECT min_date FROM min_max_dates), 
        INTERVAL n DAY
    ) AS reporting_date,
FROM 
  UNNEST(
    GENERATE_ARRAY(
        0, 
        DATE_DIFF(
            (SELECT max_date FROM min_max_dates),
            (SELECT min_date FROM min_max_dates),
            --'2021-12-31', 
            --'2015-01-01', -- Date the first account was 
            DAY
        )
    )
) AS n