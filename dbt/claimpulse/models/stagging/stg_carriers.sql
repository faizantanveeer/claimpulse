WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'raw_claims',
            'carriers'
        ) }}
)
SELECT
    carrier_id,
    TRIM(carrier_name) AS carrier_name,
    CAST (tpa_flag AS BOOLEAN) AS tpa_flag,
    _loaded_at
FROM
    source
