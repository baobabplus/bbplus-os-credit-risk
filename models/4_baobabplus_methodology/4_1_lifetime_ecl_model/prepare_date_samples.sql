/*
    This transformation is quite essential to produce an unbiaised model
    For every chunk of repayment [0-10%], [10-20%], etc... it estimates what is the sample of accounts we can use for estimating probabilities of default.
    For example, if for 40 - 50% the sample end date is 2020-12-03, it means that we must use only accounts registered prior to this date.
    Otherwise, we are going to produce biaised (selection bias) estimates, as too many accounts will not have finished paying or will not have defaulted.
*/

WITH default_history as (
    SELECT * FROM {{ref('history_defaults')}}
),

date_spine as (
    SELECT reporting_date as sample_end_date FROM {{ref('date_spine')}}
),

-- Generates a table containing 10 rows, representing the different chunks of repayment : 0 to 10%, 10 to 20%, etc...
split_chunks as (
    SELECT 
        *,
        index_chunk / 10 as perc_paid_chunk_start,
        COALESCE(
            LEAD(index_chunk / 10) OVER(ORDER BY index_chunk),
            1
        ) as perc_paid_chunk_end,
    FROM UNNEST(GENERATE_ARRAY(0, 9)) as index_chunk
),

crossjoint as (
    SELECT 
        *,
        IF(perc_paid < perc_paid_chunk_end AND has_defaulted = 0, 1, 0) as is_censored,
    FROM default_history
    CROSS JOIN split_chunks
),

grouped as (
    SELECT 
        index_chunk,
        perc_paid_chunk_start,
        perc_paid_chunk_end,
        sample_end_date,
        AVG(is_censored) as censored_percent,
    FROM crossjoint
    LEFT JOIN date_spine
    ON date_spine.sample_end_date >= crossjoint.registration_date
    GROUP BY ALL
    HAVING censored_percent <= 0.05 -- Here the tolerance to censoring is 5%.
),

get_chunck_dates as (
    SELECT 
        index_chunk,
        perc_paid_chunk_start,
        perc_paid_chunk_end,
        MAX(sample_end_date) as sample_end_date,
    FROM grouped 
    GROUP BY ALL
)

SELECT * FROM get_chunck_dates