-- Per ARCHITECTURE.md section 7: an orphaned provider_id is flagged,
-- never dropped -- a broken provider link doesn't erase a real claim
-- or deadline. The relationships test on provider_id (see schema.yml)
-- catches this at warn severity; the decision of what to DO about an
-- unmatched claim lives in intermediate, not here.

with source as (
    select * from {{ source('raw_claims', 'claims') }}
),

providers as (
    select provider_id, canonical_provider_id from {{ ref('stg_providers') }}
)

select
    source.claim_id,
    source.carrier_id,
    source.provider_id,
    providers.canonical_provider_id as resolved_provider_id,
    try_to_date(source.date_of_loss, 'YYYY-MM-DD') as date_of_loss,
    try_to_date(source.claim_open_date, 'YYYY-MM-DD') as claim_open_date,
    source.claim_status,
    source.state,
    case
        when providers.provider_id is not null then 'matched'
        else 'unmatched'
    end as provider_match_status,
    source._loaded_at
from source
left join providers
    on source.provider_id = providers.provider_id