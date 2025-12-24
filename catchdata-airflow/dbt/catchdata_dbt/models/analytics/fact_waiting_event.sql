{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    {{ dbt_utils.generate_surrogate_key([
        'id',
        'calculation_timestamp'
    ]) }}                    as event_id,
    id                        as restaurant_id,
    waiting                   as waiting_count,
    calculation_timestamp     as measured_at
from analytics.realtime_waiting

{% if is_incremental() %}
  where calculation_timestamp >
    (select max(measured_at) from {{ this }})
{% endif %}
