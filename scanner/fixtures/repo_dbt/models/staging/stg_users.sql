-- is_incremental() koennte man hier verwenden, tun wir aber nicht.
select
    user_id,
    signed_up_at
from {{ source('raw', 'users') }}
