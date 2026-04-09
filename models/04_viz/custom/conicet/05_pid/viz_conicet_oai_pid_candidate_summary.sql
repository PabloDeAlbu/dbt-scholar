WITH base AS (
    SELECT
        source_field,
        relation_parse_status,
        relation_type,
        identifier_type,
        pid_candidate_type,
        current_rule_status,
        enters_current_doi_rule,
        record_hk
    FROM {{ ref('audit_conicet_oai_record_pid_candidates') }}
),

final AS (
    SELECT
        source_field,
        COALESCE(relation_parse_status, 'Sin relation_parse_status') AS relation_parse_status,
        COALESCE(relation_type, 'Sin relation_type') AS relation_type,
        COALESCE(identifier_type, 'Sin identifier_type') AS identifier_type,
        pid_candidate_type,
        current_rule_status,
        enters_current_doi_rule,
        COUNT(*)::bigint AS row_count,
        COUNT(DISTINCT record_hk)::bigint AS record_count
    FROM base
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM final
