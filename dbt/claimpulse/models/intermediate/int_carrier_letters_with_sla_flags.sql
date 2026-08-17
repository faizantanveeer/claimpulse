WITH source_carrier_letters AS (
    SELECT
        *
    FROM
        {{ ref('stg_carrier_letters') }}
)
SELECT
    * exclude (classification_status, _loaded_at),
    (CURRENT_DATE() - response_due_date) AS overdue_days,
    CASE
        WHEN response_due_date > CURRENT_DATE() THEN 0
        ELSE 1
    END AS is_overdue,
    CASE
        WHEN classification_status = 'unclassified' THEN 'unresolved'
        ELSE classification_status
    END AS classification_status,
    CASE
        WHEN classification_status = 'unresolved' THEN 1
        ELSE 0
    END AS unresolved_letters,
    CASE
        WHEN is_overdue = 1
        AND classification_status != 'resolved'
        AND is_date_anomaly = FALSE THEN 1
        ELSE 0
    END AS is_sla_breach
FROM
    source_carrier_letters
