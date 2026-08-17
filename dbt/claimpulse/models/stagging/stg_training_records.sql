WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'raw_claims',
            'training_records'
        ) }}
)
SELECT
    training_id,
    provider_id,
    carrier_id,
    TRY_TO_DATE(
        assigned_date,
        'YYYY-MM-DD'
    ) AS assigned_date,
    TRY_TO_DATE(
        completed_date,
        'YYYY-MM-DD'
    ) AS completed_date,
    status,
    _loaded_at
FROM
    source
