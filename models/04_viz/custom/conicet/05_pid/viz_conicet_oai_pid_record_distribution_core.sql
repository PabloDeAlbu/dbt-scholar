WITH base AS (
    SELECT
        source_field,
        relation_parse_status,
        relation_type,
        identifier_type,
        pid_candidate_type,
        current_rule_status,
        enters_current_doi_rule,
        record_count,
        total_record_count,
        record_ratio
    FROM {{ ref('viz_conicet_oai_pid_record_distribution') }}
    WHERE pid_candidate_type NOT IN ('url', 'purl')
),

final AS (
    SELECT
        source_field,
        relation_parse_status,
        relation_type,
        identifier_type,
        pid_candidate_type,
        current_rule_status,
        enters_current_doi_rule,
        record_count,
        total_record_count,
        record_ratio
    FROM base
)

SELECT * FROM final
