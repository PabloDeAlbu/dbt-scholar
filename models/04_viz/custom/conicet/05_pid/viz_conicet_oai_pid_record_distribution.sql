WITH universe AS (
    SELECT COUNT(DISTINCT record_hk)::bigint AS total_record_count
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

pid_presence AS (
    SELECT DISTINCT
        source_field,
        relation_parse_status,
        relation_type,
        identifier_type,
        pid_candidate_type,
        current_rule_status,
        enters_current_doi_rule,
        record_hk
    FROM {{ ref('audit_conicet_oai_record_pid_candidates') }}
    WHERE pid_candidate_type <> 'other'
      AND pid_candidate_type <> 'empty'
),

final AS (
    SELECT
        presence.source_field,
        presence.relation_parse_status,
        COALESCE(presence.relation_type, 'Sin relation_type') AS relation_type,
        COALESCE(presence.identifier_type, 'Sin identifier_type') AS identifier_type,
        presence.pid_candidate_type,
        presence.current_rule_status,
        presence.enters_current_doi_rule,
        COUNT(DISTINCT presence.record_hk)::bigint AS record_count,
        universe.total_record_count,
        ROUND(
            COUNT(DISTINCT presence.record_hk)::numeric / NULLIF(universe.total_record_count, 0),
            4
        ) AS record_ratio
    FROM pid_presence AS presence
    CROSS JOIN universe
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 9
)

SELECT * FROM final
