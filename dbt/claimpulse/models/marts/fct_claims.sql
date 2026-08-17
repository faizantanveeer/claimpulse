WITH source AS (
    SELECT
        *
    FROM
        {{ ref('int_claims_enriched') }}
)
SELECT
    claim_id,
    carrier_id,
    provider_id,
    resolved_provider_id,
    date_of_loss,
    claim_open_date,
    claim_status,
    state,
    CASE
        WHEN resolved_provider_id IS NULL THEN 0
        ELSE 1
    END AS has_provider_link,
    COUNT(letter_id) AS letter_count,
    COALESCE(MAX(is_sla_breach), 0) AS has_sla_breach
FROM
    source
GROUP BY
    claim_id,
    carrier_id,
    provider_id,
    resolved_provider_id,
    date_of_loss,
    claim_open_date,
    claim_status,
    state
