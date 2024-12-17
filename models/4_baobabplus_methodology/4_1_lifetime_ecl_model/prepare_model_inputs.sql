/*
    Simple preparation step joining the various inputs.
*/

WITH accs as (
  SELECT 
    *,
  FROM {{ref('history_defaults')}}
  LEFT JOIN {{ref('history_segmentations')}} USING(account_id)
),

samples as (
  SELECT * FROM {{ref('prepare_date_samples')}}
),

joint as (
  SELECT 
    accs.*,
    samples.*,
    RAND() as rnd,
  FROM accs
  INNER JOIN samples
  ON 
    accs.registration_date < CAST(samples.sample_end_date AS TIMESTAMP)
)

SELECT * FROM joint