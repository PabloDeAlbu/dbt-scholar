WITH base AS (
    SELECT
        work_hk,
        work_id,
        publication_year,
        title AS title_raw,
        type AS type_raw,
        doi AS doi_raw,
        issn AS issn_raw
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
),

author_observed AS (
    SELECT
        brg.work_hk,
        CASE brg.author_position
            WHEN 'first' THEN 1
            WHEN 'middle' THEN 2
            WHEN 'last' THEN 3
            ELSE 4
        END AS author_position_sort_key,
        NULLIF(
            REPLACE(
                {{ clean_text('COALESCE(brg.canonical_display_name, brg.fallback_display_name)') }},
                '|',
                ' '
            ),
            ''
        ) AS author_name
    FROM {{ ref('brg_openalex_work_author_enriched') }} AS brg
    WHERE COALESCE(brg.canonical_display_name, brg.fallback_display_name) IS NOT NULL
),

author_dedup AS (
    SELECT
        work_hk,
        author_position_sort_key,
        author_name
    FROM (
        SELECT
            work_hk,
            author_position_sort_key,
            author_name,
            ROW_NUMBER() OVER (
                PARTITION BY work_hk, LOWER(author_name)
                ORDER BY author_position_sort_key, author_name
            ) AS rn
        FROM author_observed
        WHERE author_name IS NOT NULL
    ) AS ranked
    WHERE rn = 1
),

author_agg AS (
    SELECT
        work_hk,
        STRING_AGG(author_name, '|' ORDER BY author_position_sort_key, author_name) AS author
    FROM author_dedup
    GROUP BY work_hk
),

doi_value AS (
    SELECT DISTINCT
        base.work_hk,
        NULLIF(
            REGEXP_REPLACE(
                LOWER(TRIM(split_value)),
                '^(https?://(dx\\.)?doi\\.org/|doi:)',
                ''
            ),
            ''
        ) AS doi
    FROM base
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(base.doi_raw, ''), '|')) AS split_value
),

doi_agg AS (
    SELECT
        work_hk,
        STRING_AGG(doi, '|' ORDER BY doi) AS doi
    FROM doi_value
    WHERE doi IS NOT NULL
    GROUP BY work_hk
),

issn_value AS (
    SELECT DISTINCT
        base.work_hk,
        NULLIF(TRIM(split_value), '') AS issn
    FROM base
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(base.issn_raw, ''), '|')) AS split_value
),

issn_agg AS (
    SELECT
        work_hk,
        STRING_AGG(issn, '|' ORDER BY issn) AS issn
    FROM issn_value
    WHERE issn IS NOT NULL
    GROUP BY work_hk
),

prepared AS (
    SELECT
        'openalex'::text AS source,
        NULLIF({{ clean_text('base.work_id') }}, '')::text AS id,
        CASE
            WHEN base.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM base.publication_year)::integer
        END AS publication_year,
        NULLIF({{ clean_text('base.type_raw') }}, '')::text AS type_raw,
        NULLIF({{ clean_text('base.title_raw') }}, '')::text AS title,
        NULL::text AS subtitle,
        type_map.type::text AS type,
        author_agg.author::text AS author,
        CASE
            WHEN base.publication_year IS NOT NULL THEN LPAD(EXTRACT(YEAR FROM base.publication_year)::integer::text, 4, '0')
        END::text AS date,
        doi_agg.doi::text AS doi,
        NULL::text AS isbn,
        issn_agg.issn::text AS issn,
        NULL::text AS description
    FROM base
    LEFT JOIN author_agg
        USING (work_hk)
    LEFT JOIN doi_agg
        USING (work_hk)
    LEFT JOIN issn_agg
        USING (work_hk)
    LEFT JOIN {{ ref('dim_unlp_publication_dedup_type') }} AS type_map
        ON type_map.source = 'openalex'
       AND LOWER(BTRIM(type_map.type_raw)) = LOWER(BTRIM(base.type_raw))
),

final AS (
    SELECT
        source,
        id,
        publication_year,
        type_raw,
        title,
        subtitle,
        type,
        author,
        date,
        doi,
        isbn,
        issn,
        description,
        (
            title IS NOT NULL
            AND type IS NOT NULL
            AND author IS NOT NULL
            AND publication_year IS NOT NULL
        ) AS dedup_eligible
    FROM prepared
)

SELECT *
FROM final
