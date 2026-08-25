{{ config(materialized='view') }}

WITH period AS (
    SELECT
        (EXTRACT(YEAR FROM CURRENT_DATE)::integer - 4) AS period_start_year,
        EXTRACT(YEAR FROM CURRENT_DATE)::integer AS period_end_year
),

authorship AS (
    SELECT fact.*
    FROM {{ ref('fct_unlp_portalderevistas_journal_article_author') }} AS fact
    CROSS JOIN period
    WHERE fact.publication_year BETWEEN period.period_start_year AND period.period_end_year
),

author_status AS (
    SELECT
        journal_id,
        author_id,
        CASE
            WHEN BOOL_OR(affiliation_status = 'unlp') THEN 'unlp'
            WHEN BOOL_OR(affiliation_status = 'external') THEN 'external'
            ELSE 'unknown'
        END AS affiliation_status
    FROM authorship
    GROUP BY journal_id, author_id
),

authorship_stats AS (
    SELECT
        authorship.journal_id,
        MIN(authorship.journal_title) AS journal_title,
        MIN(period.period_start_year) AS period_start_year,
        MIN(period.period_end_year) AS period_end_year,
        COUNT(DISTINCT authorship.item_id)::bigint AS article_count,
        COUNT(*)::bigint AS authorship_count,
        COUNT(*) FILTER (WHERE authorship.affiliation_status = 'unlp')::bigint AS unlp_authorship_count,
        COUNT(*) FILTER (WHERE authorship.affiliation_status = 'external')::bigint AS external_authorship_count,
        COUNT(*) FILTER (WHERE authorship.affiliation_status = 'unknown')::bigint AS unknown_authorship_count
    FROM authorship
    CROSS JOIN period
    GROUP BY authorship.journal_id
),

author_stats AS (
    SELECT
        journal_id,
        COUNT(*)::bigint AS unique_author_count,
        COUNT(*) FILTER (WHERE affiliation_status = 'unlp')::bigint AS unique_unlp_author_count,
        COUNT(*) FILTER (WHERE affiliation_status = 'external')::bigint AS unique_external_author_count,
        COUNT(*) FILTER (WHERE affiliation_status = 'unknown')::bigint AS unique_unknown_author_count
    FROM author_status
    GROUP BY journal_id
),

final AS (
    SELECT
        authorship.journal_id,
        authorship.journal_title,
        authorship.period_start_year,
        authorship.period_end_year,
        authorship.article_count,
        authorship.authorship_count,
        authorship.unlp_authorship_count,
        authorship.external_authorship_count,
        authorship.unknown_authorship_count,
        (authorship.unlp_authorship_count + authorship.external_authorship_count)::bigint
            AS classified_authorship_count,
        ROUND(
            100.0 * authorship.unlp_authorship_count
            / NULLIF(authorship.unlp_authorship_count + authorship.external_authorship_count, 0),
            2
        ) AS unlp_authorship_share_pct,
        ROUND(
            100.0 * (authorship.unlp_authorship_count + authorship.external_authorship_count)
            / authorship.authorship_count,
            2
        ) AS known_affiliation_coverage_pct,
        author.unique_author_count,
        author.unique_unlp_author_count,
        author.unique_external_author_count,
        author.unique_unknown_author_count
    FROM authorship_stats AS authorship
    INNER JOIN author_stats AS author
        USING (journal_id)
)

SELECT *
FROM final
