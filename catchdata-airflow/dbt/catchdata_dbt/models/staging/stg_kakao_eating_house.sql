{{ config(materialized='table') }}

select
  *,
  regexp_extract(filename, '_(\\d{8})\\.csv', 1) as batch_date
from read_csv_auto(
  's3://team5-batch/raw_data/kakao/*.csv',
  header=true,
  filename=true
)
