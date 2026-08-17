WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw_claims', 'training_records') }}
),

providers AS (
    SELECT
        provider_id,
        resolved_provider_id
    FROM
        {{ ref('stg_providers') }}
)

SELECT
    training_id,
    source.provider_id,
    providers.resolved_provider_id,
    carrier_id,
    TRY_TO_DATE(assigned_date, 'YYYY-MM-DD') AS assigned_date,
    TRY_TO_DATE(completed_date, 'YYYY-MM-DD') AS completed_date,
    status,
    _loaded_at
FROM
    source
LEFT JOIN providers
    ON source.provider_id = providers.provider_id