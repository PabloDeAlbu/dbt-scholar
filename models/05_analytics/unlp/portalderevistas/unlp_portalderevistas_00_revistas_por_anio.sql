{{ config(materialized='view') }}

WITH item_stats AS (
    SELECT
        journal_id,
        MIN(journal_title) AS journal_title,
        publication_year,
        COUNT(*)::bigint AS published_item_count,
        COUNT(*) FILTER (WHERE is_article)::bigint AS article_item_count,
        COUNT(*) FILTER (WHERE NOT is_article)::bigint AS other_item_count
    FROM {{ ref('fct_unlp_portalderevistas_journal_item') }}
    WHERE publication_year IS NOT NULL
    GROUP BY journal_id, publication_year
),

author_stats AS (
    SELECT
        journal_id,
        publication_year,
        COUNT(DISTINCT item_id)::bigint AS article_with_author_count,
        COUNT(*)::bigint AS authorship_count,
        COUNT(DISTINCT author_id)::bigint AS author_identity_count
    FROM {{ ref('fct_unlp_portalderevistas_journal_article_author') }}
    GROUP BY journal_id, publication_year
),

final AS (
    SELECT
        item.journal_id,
        item.journal_title,
        item.publication_year,
        item.published_item_count,
        item.article_item_count,
        item.other_item_count,
        COALESCE(author.article_with_author_count, 0)::bigint
            AS article_with_author_count,
        (
            item.article_item_count
            - COALESCE(author.article_with_author_count, 0)
        )::bigint AS article_without_author_count,
        COALESCE(author.authorship_count, 0)::bigint AS authorship_count,
        COALESCE(author.author_identity_count, 0)::bigint AS author_identity_count,
        ROUND(
            100.0 * COALESCE(author.article_with_author_count, 0)
            / NULLIF(item.article_item_count, 0),
            2
        ) AS article_author_coverage_pct
    FROM item_stats AS item
    LEFT JOIN author_stats AS author
        USING (journal_id, publication_year)
)

SELECT *
FROM final
