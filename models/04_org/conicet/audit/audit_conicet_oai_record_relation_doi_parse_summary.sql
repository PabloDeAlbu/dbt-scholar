{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('audit_conicet_oai_record_relation_doi_parse') }}
),

final AS (
    SELECT
        doi_parse_pattern,
        COUNT(*) AS row_count,
        COUNT(DISTINCT record_hk) AS record_count
    FROM base
    GROUP BY doi_parse_pattern
)

SELECT * FROM final
