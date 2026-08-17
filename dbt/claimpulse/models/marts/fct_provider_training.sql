WITH int_provider_training AS (
    SELECT
        *
    FROM
        {{ ref('int_provider_training_status') }}
)
SELECT
    *
FROM
    int_provider_training
