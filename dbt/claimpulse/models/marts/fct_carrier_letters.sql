WITH int_carrier_letter AS (
    SELECT
        *
    FROM {{ ref('int_carrier_letters_with_sla_flags') }}
),

stg_claims AS (
    SELECT
        *
    FROM {{ ref('stg_claims') }}
)

SELECT
    letters.*,
    claims.resolved_provider_id
FROM int_carrier_letter AS letters
LEFT JOIN stg_claims AS claims
    ON letters.claim_id = claims.claim_id