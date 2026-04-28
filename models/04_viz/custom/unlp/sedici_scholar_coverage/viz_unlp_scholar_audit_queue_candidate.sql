{{ config(materialized='table') }}

WITH unioned AS (
    SELECT * FROM {{ ref('int_unlp_scholar_audit_recent_items') }}
    UNION ALL
    SELECT * FROM {{ ref('int_unlp_scholar_audit_openaire_visible') }}
    UNION ALL
    SELECT * FROM {{ ref('int_unlp_scholar_audit_openalex_doi_visible') }}
    UNION ALL
    SELECT * FROM {{ ref('int_unlp_scholar_audit_high_value_no_pid') }}
    UNION ALL
    SELECT * FROM {{ ref('int_unlp_scholar_audit_legacy_backfile') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY audit_scenario
            ORDER BY
                CASE audit_scenario
                    WHEN 'recent_items' THEN date_accessioned
                    WHEN 'openaire_visible' THEN date_accessioned
                    WHEN 'openalex_doi_visible' THEN date_accessioned
                    WHEN 'high_value_no_pid' THEN date_accessioned
                    WHEN 'legacy_backfile' THEN NULL
                END DESC NULLS LAST,
                CASE
                    WHEN audit_scenario = 'legacy_backfile' THEN date_accessioned
                END ASC NULLS LAST,
                CASE
                    WHEN audit_scenario = 'high_value_no_pid' THEN author_count
                END DESC NULLS LAST,
                CASE
                    WHEN audit_scenario = 'legacy_backfile' THEN item_id
                END ASC,
                CASE
                    WHEN audit_scenario <> 'legacy_backfile' THEN item_id
                END DESC
        ) AS selection_order
    FROM unioned
),

final AS (
    SELECT
        audit_scenario,
        cohort_rule,
        selection_order,
        item_hk,
        item_id,
        institutional_uri,
        title,
        publication_year,
        publication_type,
        date_issued,
        date_accessioned,
        author_count,
        has_doi,
        doi_count,
        doi,
        has_handle,
        handle_count,
        handle,
        matched_by_unique_doi,
        matched_by_unique_original_id,
        owning_collection_title,
        owning_community_title,
        owning_root_community_title
    FROM ranked
    WHERE selection_order <= 10
)

SELECT * FROM final
