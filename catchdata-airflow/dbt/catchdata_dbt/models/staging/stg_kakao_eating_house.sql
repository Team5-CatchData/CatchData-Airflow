{{ config(materialized='table') }}

LOAD httpfs;

SET s3_region='ap-northeast-2';
SET s3_use_ssl=true;

select
  *,
  regexp_extract(filename, '_([0-9]{8})\.csv', 1) as batch_date
from read_csv_auto(
  's3://team5-batch/raw_data/kakao/*.csv',
  header=true,
  filename=true
)
