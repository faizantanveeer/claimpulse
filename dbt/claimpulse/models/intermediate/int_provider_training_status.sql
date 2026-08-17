WITH source_training_records AS (
    SELECT
        *
    FROM
        {{ ref('stg_training_records') }}
)
SELECT
    CASE
        WHEN CURRENT_DATE() - assigned_date > 30
        AND status = 'pending' THEN 1
        ELSE 0
    END AS is_training_lapse,
    CASE
        WHEN status = 'completed' THEN completed_date - assigned_date
        WHEN status = 'pending' THEN (CURRENT_DATE() - assigned_date)
        ELSE NULL
    END AS training_period,
        CASE
            WHEN (
                completed_date < assigned_date
                AND status = 'completed'
            )
            OR (
                status = 'completed'
                AND completed_date IS NULL
            ) THEN 1
            ELSE 0
        END AS is_training_anomaly,* exclude _loaded_at
        FROM
            source_training_records
