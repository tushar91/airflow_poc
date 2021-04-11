#standardSQL
-- Query to create the final Dimension currency table
-- To get the last updated one euro value from the table
WITH history_rank AS (SELECT
  cur_code,
  serial_code,
  last_updated_date,
  one_euro_value,
  RANK() OVER (PARTITION BY cur_code ORDER BY last_updated_date DESC) AS rank_curency
FROM
  airflow_poc.raw_dimension_currency)

SELECT
  cur_code,
  one_euro_value,
  last_updated_date
  serial_code
FROM history_rank
WHERE rank_curency = 1;
