{{ config(materialized='view') }}

WITH author_stats AS (
    SELECT
        item_id,
        COUNT(DISTINCT author_id)::bigint AS recognized_author_count,
        COUNT(DISTINCT author_id) FILTER (
            WHERE has_authority_control
        )::bigint AS authority_author_count,
        COUNT(DISTINCT author_id) FILTER (
            WHERE has_voc_match
        )::bigint AS voc_author_count
    FROM {{ ref('brg_libros_unlp_journal_item_author') }}
    GROUP BY item_id
)

SELECT
    item.item_id,
    item.handle,
    item.item_url,
    item.item_title,
    item.dc_type,
    item.sedici_subtype,
    item.publication_date,
    item.publication_year,
    item.journal_id,
    item.journal_title,
    item.owning_community_path_titles,
    item.is_article,
    item.publication_year IS NOT NULL AS has_publication_year,
    COALESCE(author.recognized_author_count, 0)::bigint
        AS recognized_author_count,
    COALESCE(author.authority_author_count, 0)::bigint
        AS authority_author_count,
    COALESCE(author.voc_author_count, 0)::bigint AS voc_author_count,
    COALESCE(author.recognized_author_count, 0) > 0
        AS has_recognized_author,
    COALESCE(author.authority_author_count, 0) > 0
        AS has_authority_author,
    COALESCE(author.voc_author_count, 0) > 0 AS has_voc_author,
    unit.unlp_unit_id,
    unit.unlp_unit_name,
    unit.unlp_unit_item_count,
    unit.unlp_unit_coverage_pct,
    unit.unlp_unit_attribution_status
FROM {{ ref('fct_libros_unlp_journal_item') }} AS item
LEFT JOIN author_stats AS author
    USING (item_id)
LEFT JOIN {{ ref('unlp_libros_00_revistas_unidad') }} AS unit
    USING (journal_id)
