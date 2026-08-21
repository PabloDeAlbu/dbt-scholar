{{ config(materialized='view') }}

WITH item_stats AS (
    SELECT
        journal_id,
        MIN(journal_title) AS journal_title,
        COUNT(*)::bigint AS published_item_count,
        COUNT(*) FILTER (WHERE is_article)::bigint AS article_item_count,
        COUNT(*) FILTER (
            WHERE is_article
              AND publication_date IS NOT NULL
              AND publication_year IS NOT NULL
        )::bigint AS dated_article_count,
        COUNT(*) FILTER (
            WHERE is_article
              AND (publication_date IS NULL OR publication_year IS NULL)
        )::bigint AS undated_article_count,
        COUNT(*) FILTER (WHERE NOT is_article)::bigint AS other_item_count,
        MIN(publication_year) AS first_publication_year,
        MAX(publication_year) AS last_publication_year
    FROM {{ ref('fct_libros_unlp_journal_item') }}
    GROUP BY journal_id
),

author_stats AS (
    SELECT
        journal_id,
        COUNT(DISTINCT item_id)::bigint AS article_with_author_count,
        COUNT(*)::bigint AS authorship_count,
        COUNT(DISTINCT author_id)::bigint AS author_identity_count,
        COUNT(DISTINCT author_id) FILTER (
            WHERE has_authority_control
        )::bigint AS authority_identity_count,
        COUNT(DISTINCT author_id) FILTER (
            WHERE has_voc_match
        )::bigint AS voc_identity_count
    FROM {{ ref('fct_libros_unlp_journal_article_author') }}
    GROUP BY journal_id
),

final AS (
    SELECT
        item.journal_id,
        item.journal_title,
        item.published_item_count,
        item.article_item_count,
        item.dated_article_count,
        item.undated_article_count,
        item.other_item_count,
        COALESCE(author.article_with_author_count, 0)::bigint
            AS article_with_author_count,
        (
            item.dated_article_count
            - COALESCE(author.article_with_author_count, 0)
        )::bigint AS article_without_author_count,
        COALESCE(author.authorship_count, 0)::bigint AS authorship_count,
        COALESCE(author.author_identity_count, 0)::bigint AS author_identity_count,
        COALESCE(author.authority_identity_count, 0)::bigint AS authority_identity_count,
        COALESCE(author.voc_identity_count, 0)::bigint AS voc_identity_count,
        ROUND(
            100.0 * COALESCE(author.article_with_author_count, 0)
            / NULLIF(item.dated_article_count, 0),
            2
        ) AS article_author_coverage_pct,
        item.first_publication_year,
        item.last_publication_year
    FROM item_stats AS item
    LEFT JOIN author_stats AS author
        USING (journal_id)
)

SELECT *
FROM final
