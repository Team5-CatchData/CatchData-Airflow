{{ config(materialized='table') }}

select
    date(measured_at)                 as snapshot_date,
    restaurant_id,
    avg(waiting_count)                as avg_waiting,
    max(waiting_count)                as max_waiting,
    count(*)                          as waiting_event_count
from {{ ref('fact_waiting_event') }}
group by 1, 2