{{ config(materialized='table') }}

WITH source_scope AS (
    SELECT
        institution_ror,
        source_label,
        COUNT(*) AS item_count
    FROM {{ ref('fct_dspacedb5_item_extraction') }}
    GROUP BY institution_ror, source_label
),
institution_summary AS (
    SELECT
        institution_ror,
        COUNT(*) AS issue_count,
        STRING_AGG(source_label, ' | ' ORDER BY source_label) AS source_label_values,
        SUM(item_count) AS total_item_observations
    FROM source_scope
    GROUP BY institution_ror
    HAVING COUNT(*) > 1
)

SELECT * FROM institution_summary
