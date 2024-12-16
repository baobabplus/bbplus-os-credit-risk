WITH wo as (
    SELECT * FROM {{ref('raw_write_offs')}}
)

-- Deduplication step in case there are several write offs or repossessions on the same account
SELECT 
    account_id,
    write_off_status,
    MIN(CAST(changed_date AS TIMESTAMP)) as changed_date, 
FROM wo
GROUP BY ALL