-- Resolves duplicate providers (typo'd whitespace, same person) to a
-- single canonical provider_id, per ARCHITECTURE.md section 7. This is
-- the one staging model that rewrites an identity rather than just
-- flagging a problem -- because deduplication is a modeling decision,
-- not a data correction.

with source as (
    select * from {{ source('raw_claims', 'providers') }}
),

cleaned as (
    select
        provider_id,
        {{ normalize_text('provider_name') }} as normalized_name,
        provider_name as original_provider_name,
        nullif(trim(specialty), '') as specialty,
        state,
        _loaded_at
    from source
),

-- group near-duplicates by normalized name + state, pick the earliest
-- provider_id (lexically smallest) as the canonical one
canonical as (
    select
        *,
        min(provider_id) over (
            partition by normalized_name, state
        ) as resolved_provider_id
    from cleaned
)

select
    provider_id,
    resolved_provider_id,
    (provider_id != resolved_provider_id) as is_duplicate,
    original_provider_name as provider_name,
    coalesce(specialty, 'unknown') as specialty,
    (specialty is null) as specialty_was_missing,
    state,
    _loaded_at
from canonical