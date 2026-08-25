{{ config(materialized='view') }}

WITH authorship AS (
    SELECT *
    FROM {{ ref('fct_unlp_portalderevistas_journal_article_author') }}
),

grouped AS (
    SELECT
        affiliation_status,
        affiliation_evidence,
        COUNT(*)::bigint AS authorship_count,
        COUNT(DISTINCT item_id)::bigint AS article_count,
        COUNT(DISTINCT author_id)::bigint AS unique_author_count,
        COUNT(*) FILTER (WHERE has_authority_control)::bigint AS authority_control_count,
        COUNT(*) FILTER (WHERE has_voc_match)::bigint AS voc_match_count
    FROM authorship
    GROUP BY affiliation_status, affiliation_evidence
),

final AS (
    SELECT
        *,
        ROUND(
            100.0 * authorship_count / SUM(authorship_count) OVER (),
            2
        ) AS authorship_share_pct,
        ROUND(
            100.0 * authority_control_count / authorship_count,
            2
        ) AS authority_coverage_pct,
        ROUND(
            100.0 * voc_match_count / authorship_count,
            2
        ) AS voc_match_coverage_pct
    FROM grouped
)

SELECT *
FROM final
