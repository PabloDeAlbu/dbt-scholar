-- ¿Qué candidatos COAR distintos aporta cada record OAI de CONICET?
WITH record_candidates AS (
    SELECT
        record_hk,
        COUNT(*)::int AS candidate_count,
        COUNT(*) FILTER (
            WHERE resource_type_uri <> 'http://purl.org/coar/resource_type/c_1843'
        )::int AS specific_candidate_count,
        MIN(resource_type_uri) FILTER (
            WHERE resource_type_uri <> 'http://purl.org/coar/resource_type/c_1843'
        ) AS specific_resource_type_uri,
        MIN(resource_type_uri) AS only_resource_type_uri
    FROM {{ ref('brg_conicet_oai_record_resource_type') }}
    GROUP BY record_hk
),

-- ¿Qué URI puede elegirse sin priorizar un vocabulario sobre otro?
expected_resolution AS (
    SELECT
        record_hk,
        candidate_count,
        CASE
            WHEN specific_candidate_count = 1 THEN specific_resource_type_uri
            WHEN specific_candidate_count = 0 AND candidate_count = 1
                THEN only_resource_type_uri
        END AS resource_type_uri
    FROM record_candidates
),

-- ¿Qué selecciones difieren de la regla declarada para los candidatos del bridge?
selection_mismatch AS (
    SELECT COALESCE(selected.record_hk, expected.record_hk) AS record_hk
    FROM {{ ref('int_conicet_oai_record_resource_type_selected') }} AS selected
    FULL OUTER JOIN expected_resolution AS expected
        USING (record_hk)
    WHERE selected.resource_type_uri IS DISTINCT FROM expected.resource_type_uri
       OR selected.resource_type_count IS DISTINCT FROM expected.candidate_count
       OR selected.has_multiple_resource_types
            IS DISTINCT FROM (expected.candidate_count > 1)
),

-- ¿Qué records publicados difieren de la selección tipológica intermedia?
publication_mismatch AS (
    SELECT record.record_hk
    FROM {{ ref('fct_conicet_oai_record_publication') }} AS record
    LEFT JOIN {{ ref('int_conicet_oai_record_resource_type_selected') }} AS selected
        USING (record_hk)
    WHERE record.resource_type_uri IS DISTINCT FROM selected.resource_type_uri
       OR record.resource_type IS DISTINCT FROM selected.resource_type
       OR record.resource_type_label_es IS DISTINCT FROM selected.resource_type_label_es
       OR record.resource_type_source IS DISTINCT FROM selected.resource_type_source
       OR record.resource_type_source_dc_types IS DISTINCT FROM selected.resource_type_source_dc_types
       OR record.resource_type_count IS DISTINCT FROM COALESCE(selected.resource_type_count, 0)
       OR record.has_multiple_resource_types
            IS DISTINCT FROM COALESCE(selected.has_multiple_resource_types, false)
       OR record.has_resource_type
            IS DISTINCT FROM COALESCE(selected.has_resource_type, false)
),

-- ¿Qué records incumplen la resolución o su proyección en el hecho OAI?
final AS (
    SELECT record_hk FROM selection_mismatch
    UNION
    SELECT record_hk FROM publication_mismatch
)

SELECT *
FROM final
