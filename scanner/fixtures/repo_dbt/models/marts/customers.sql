-- Inkrementell konfiguriert, aber ohne is_incremental(): jeder Lauf schreibt alles neu.
select
    customer_id,
    updated_at
from {{ ref('stg_customers') }}
