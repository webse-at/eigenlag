{{ config(materialized='incremental', unique_key='event_id') }}

select
    event_id,
    received_at
from {{ source('raw', 'events') }}

{% if is_incremental() %}
where received_at > (select max(received_at) from {{ this }})
{% endif %}
