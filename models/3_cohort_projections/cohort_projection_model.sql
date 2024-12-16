WITH cohorts as (
    SELECT 
        *, 
        (reporting_day - 1) / 30 as reporting_month, 
        CASE
            WHEN reporting_day = 1 THEN amount_paid_percent
            ELSE amount_paid_percent - LAG(amount_paid_percent) OVER(PARTITION BY cohort_month ORDER BY reporting_day)
        END as amount_paid_percent_incr, 
    FROM {{ref('cohorts_beginner')}}
),

-- Step 1 : Building a reference dataset : containing for each month, the reference point for the last 6 available cohorts
last_6_reference as (
    SELECT 
        *,
    FROM cohorts
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(reporting_month AS INT64) ORDER BY cohort_month DESC) <= 6
),

last_6_aggregated as (
    SELECT 
        reporting_month,
        AVG(amount_paid_percent_incr)                           as projected_paid_percent_incr,
        AVG(amount_paid_percent - amount_paid_percent_incr)     as reference_paid_percent, -- this field will serve to apply the scale factor
    FROM last_6_reference
    GROUP BY ALL
),

-- Step 2 : Let's build the full dataset of cohorts including future months
cohort_months as (
    SELECT DISTINCT cohort_month FROM cohorts
),

reporting_months as (
    SELECT DISTINCT reporting_month FROM cohorts
),

full_cohort_spine as (
    SELECT * FROM cohort_months
    CROSS JOIN reporting_months
),

-- Step 3 : Joining the data back in this target dataset
joint as (
    SELECT 
        full_cohort_spine.*,
        cohorts.amount_paid_percent,
        cohorts.amount_paid_percent_incr,
    FROM full_cohort_spine 
    LEFT JOIN cohorts USING(cohort_month, reporting_month)
),

joint_with_projections as (
    SELECT 
        joint.*,
        last_6_aggregated.projected_paid_percent_incr,
        last_6_aggregated.reference_paid_percent,
        MAX(joint.amount_paid_percent) OVER(PARTITION BY cohort_month) / MIN(last_6_aggregated.reference_paid_percent) OVER(PARTITION BY cohort_month) as scale_factor,
    FROM joint 
    LEFT JOIN last_6_aggregated
    ON 
        joint.amount_paid_percent IS NULL AND 
        joint.reporting_month = last_6_aggregated.reporting_month
),

-- Step 4 : combine actuals and predictions, and calculate cumulative values
projected_and_actuals as (
    SELECT 
    *,
    COALESCE(amount_paid_percent_incr, projected_paid_percent_incr)                 as projected_and_actual_amount_paid_percent_incr,
    COALESCE(amount_paid_percent_incr, projected_paid_percent_incr * scale_factor)  as projected_and_actual_amount_paid_percent_incr_with_scale_factor,
    FROM joint_with_projections 
    GROUP BY ALL
)

SELECT 
    *,
    SUM(projected_and_actual_amount_paid_percent_incr) OVER(
        PARTITION BY cohort_month ORDER BY reporting_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as projected_and_actual_amount_paid_percent,
    SUM(projected_and_actual_amount_paid_percent_incr_with_scale_factor) OVER(
        PARTITION BY cohort_month ORDER BY reporting_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as projected_and_actual_amount_paid_percent_with_scale_factor,
FROM projected_and_actuals
ORDER BY 1, 2