with source_provider as (
    select * from {{ ref('stg_providers') }}
)
select
    provider_id,
    resolved_provider_id,
    provider_name,
    specialty,
    specialty_was_missing,
    state,
    _loaded_at
from source_provider
where provider_id = resolved_provider_id