WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'raw_claims',
            'carrier_letters'
        ) }}
)
SELECT
    letter_id,
    claim_id,
    carrier_id,
    letter_type,
    TRY_TO_DATE(
        received_date,
        'YYYY-MM-DD'
    ) AS received_date,
    TRY_TO_DATE(
        response_due_date,
        'YYYY-MM-DD'
    ) AS response_due_date,
    COALESCE(NULLIF(TRIM(classification_status), ''), 'unclassified') AS classification_status,
    (
        classification_status IS NULL
        OR TRIM(classification_status) = ''
    ) AS was_unclassified,
    (
        response_due_date < received_date
    ) AS is_date_anomaly,
    _loaded_at
FROM
    source
