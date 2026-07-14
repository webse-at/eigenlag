select
    session_id,
    started_at
from {{ source('raw', 'sessions') }}

{% if is_incremental() %}
where started_at > (select max(started_at) from {{ this }})
{% endif %}
