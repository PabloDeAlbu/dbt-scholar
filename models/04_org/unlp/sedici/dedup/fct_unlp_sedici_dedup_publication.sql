WITH base AS (
    SELECT
        item_hk,
        item_id,
        dc_identifier_uri,
        date_issued,
        title AS title_raw,
        subtitle AS subtitle_raw,
        type AS type_raw,
        doi AS doi_raw,
        isbn AS isbn_raw,
        issn AS issn_raw,
        description AS description_raw
    FROM {{ ref('fct_unlp_sedici_item_publication') }}
),

author_observed AS (
    SELECT
        brg.item_hk,
        brg.sedici_author_hk,
        COALESCE(brg.author_place, 2147483647) AS author_place,
        NULLIF(
            REPLACE({{ clean_text('dim.author_name_preferred') }}, '|', ' '),
            ''
        ) AS author_name
    FROM {{ ref('brg_unlp_sedici_item_author') }} AS brg
    INNER JOIN {{ ref('dim_unlp_sedici_author') }} AS dim
        ON dim.sedici_author_hk = brg.sedici_author_hk
),

author_dedup AS (
    SELECT
        item_hk,
        author_place,
        author_name
    FROM (
        SELECT
            item_hk,
            author_place,
            author_name,
            ROW_NUMBER() OVER (
                PARTITION BY item_hk, LOWER(author_name)
                ORDER BY author_place, author_name
            ) AS rn
        FROM author_observed
        WHERE author_name IS NOT NULL
    ) AS ranked
    WHERE rn = 1
),

author_agg AS (
    SELECT
        item_hk,
        STRING_AGG(author_name, '|' ORDER BY author_place, author_name) AS author
    FROM author_dedup
    GROUP BY item_hk
),

doi_value AS (
    SELECT DISTINCT
        base.item_hk,
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
        item_hk,
        STRING_AGG(doi, '|' ORDER BY doi) AS doi
    FROM doi_value
    WHERE doi IS NOT NULL
    GROUP BY item_hk
),

isbn_value AS (
    SELECT DISTINCT
        base.item_hk,
        NULLIF(TRIM(split_value), '') AS isbn
    FROM base
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(base.isbn_raw, ''), '|')) AS split_value
),

isbn_agg AS (
    SELECT
        item_hk,
        STRING_AGG(isbn, '|' ORDER BY isbn) AS isbn
    FROM isbn_value
    WHERE isbn IS NOT NULL
    GROUP BY item_hk
),

issn_value AS (
    SELECT DISTINCT
        base.item_hk,
        NULLIF(TRIM(split_value), '') AS issn
    FROM base
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(base.issn_raw, ''), '|')) AS split_value
),

issn_agg AS (
    SELECT
        item_hk,
        STRING_AGG(issn, '|' ORDER BY issn) AS issn
    FROM issn_value
    WHERE issn IS NOT NULL
    GROUP BY item_hk
),

prepared AS (
    SELECT
        'sedici'::text AS source,
        NULLIF(
            REGEXP_REPLACE(
                TRIM(base.dc_identifier_uri),
                '^https?://[^/]+/handle/',
                ''
            ),
            ''
        )::text AS id,
        CASE
            WHEN base.date_issued IS NOT NULL THEN EXTRACT(YEAR FROM base.date_issued)::integer
        END AS publication_year,
        NULLIF({{ clean_text('base.type_raw') }}, '')::text AS type_raw,
        NULLIF({{ clean_text('base.title_raw') }}, '')::text AS title,
        NULLIF({{ clean_text('base.subtitle_raw') }}, '')::text AS subtitle,
        type_map.type::text AS type,
        author_agg.author::text AS author,
        CASE
            WHEN base.date_issued IS NOT NULL THEN LPAD(EXTRACT(YEAR FROM base.date_issued)::integer::text, 4, '0')
        END::text AS date,
        doi_agg.doi::text AS doi,
        isbn_agg.isbn::text AS isbn,
        issn_agg.issn::text AS issn,
        NULLIF({{ clean_text('base.description_raw') }}, '')::text AS description
    FROM base
    LEFT JOIN author_agg
        USING (item_hk)
    LEFT JOIN doi_agg
        USING (item_hk)
    LEFT JOIN isbn_agg
        USING (item_hk)
    LEFT JOIN issn_agg
        USING (item_hk)
    LEFT JOIN {{ ref('dim_unlp_publication_dedup_type') }} AS type_map
        ON type_map.source = 'sedici'
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
