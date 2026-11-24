{{ config(materialized = 'table') }}

-- ¿Cuántos tipos COAR candidatos y cuántos tipos específicos posee cada record?
WITH candidate_stats AS (
    SELECT
        record_hk,
        COUNT(*)::int AS resource_type_count,
        COUNT(*) FILTER (
            WHERE resource_type_uri <> 'http://purl.org/coar/resource_type/c_1843'
        )::int AS specific_resource_type_count
    FROM {{ ref('brg_conicet_oai_record_resource_type') }}
    GROUP BY record_hk
),

-- ¿Qué candidato puede representar al record sin priorizar un vocabulario?
selected_candidate AS (
    SELECT candidate.*
    FROM {{ ref('brg_conicet_oai_record_resource_type') }} AS candidate
    INNER JOIN candidate_stats AS stats
        USING (record_hk)
    WHERE (
        stats.specific_resource_type_count = 1
        AND candidate.resource_type_uri <> 'http://purl.org/coar/resource_type/c_1843'
    ) OR (
        stats.specific_resource_type_count = 0
        AND stats.resource_type_count = 1
    )
),

-- ¿Qué resolución tipológica y qué evidencia de multiplicidad expone cada record?
final AS (
    SELECT
        stats.record_hk,
        selected.resource_type_uri,
        selected.resource_type,
        selected.resource_type_label_es,
        selected.type_vocabularies AS resource_type_source,
        selected.source_dc_types AS resource_type_source_dc_types,
        stats.resource_type_count,
        stats.resource_type_count > 1 AS has_multiple_resource_types,
        selected.resource_type_uri IS NOT NULL AS has_resource_type
    FROM candidate_stats AS stats
    LEFT JOIN selected_candidate AS selected
        USING (record_hk)
)

SELECT *
FROM final
