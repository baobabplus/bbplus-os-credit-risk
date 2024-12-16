/*
    This transformations is generating a dataset containing one row for each day betwee : 
    - The first day you opened an account
    - Today. With artificial data here we consider today being the day after we received the last payment.
*/

WITH payments as (
    SELECT * FROM {{ref('cleaned_payments')}}
),

min_max_dates as (
    SELECT 
        CAST(MIN(payment_effective_date)            AS DATE)                    as min_date,
        DATE_ADD(CAST(MAX(payment_effective_date)   AS DATE), INTERVAL 1 DAY)   as max_date,
    FROM payments
)

SELECT
    CAST(
        TIMESTAMP_ADD(
            (SELECT min_date FROM min_max_dates), 
            INTERVAL n DAY
        ) AS TIMESTAMP
    ) AS reporting_date,
FROM 
  UNNEST(
    GENERATE_ARRAY(
        0, 
        DATE_DIFF(
            (SELECT max_date FROM min_max_dates),
            (SELECT min_date FROM min_max_dates),
            DAY
        )
    )
) AS n