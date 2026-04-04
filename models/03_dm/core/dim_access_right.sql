{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        coar_uri AS access_right_uri,
        coar_label AS access_right_label,
        coar_label_es AS access_right_label_es,
        dc_right
    FROM {{ ref('seed_coar_access_right') }}
)

SELECT * FROM base
