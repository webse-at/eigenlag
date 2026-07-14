/*
    Frueher stand hier ein is_incremental()-Block.
    Er wurde entfernt, der Kommentar blieb.
*/
select
    id,
    updated_at
from {{ ref('stg_legacy') }}
