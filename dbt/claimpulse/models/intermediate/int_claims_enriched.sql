WITH claims AS (
    SELECT
        *
    FROM
        {{ ref('stg_claims') }}
), 
int_carrier_letters as (
    select * from {{ ref('int_carrier_letters_with_sla_flags') }}
)
SELECT
    claims.claim_id, claims.carrier_id, claims.provider_id, claims.resolved_provider_id, claims.date_of_loss, claims.claim_open_date, claims.claim_status, claims.state, int_carrier_letters.letter_id,int_carrier_letters.is_sla_breach
FROM
    claims
left join int_carrier_letters 
on int_carrier_letters.claim_id = claims.claim_id
