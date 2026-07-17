WITH base AS (
    SELECT
        item_hk,
        item_id,
        dc_identifier_uri,
        dc_date_available,
        date_issued,
        title AS title_raw,
        subtitle AS subtitle_raw,
        type AS type_raw,
        subtype AS subtype_raw,
        doi AS doi_raw,
        isbn AS isbn_raw,
        issn AS issn_raw,
        description AS description_raw,
        subject AS subject_raw,
        owning_root_community_id,
        owning_root_community_title,
        owning_community_path_titles,
        owning_community_id,
        owning_community_title,
        owning_collection,
        owning_collection_title
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
        NULLIF({{ clean_text('base.type_raw') }}, '')::text AS type,
        NULLIF({{ clean_text('base.subtype_raw') }}, '')::text AS subtype,
        base.date_issued AS date,
        base.dc_date_available,
        NULLIF({{ clean_text('base.title_raw') }}, '')::text AS title,
        NULLIF({{ clean_text('base.subtitle_raw') }}, '')::text AS subtitle,
        type_map.type_dedup::text AS type_dedup,
        author_agg.author::text AS author,
        doi_agg.doi::text AS doi,
        issn_agg.issn::text AS issn,
        isbn_agg.isbn::text AS isbn,
        NULLIF({{ clean_text('base.subject_raw') }}, '')::text AS subject,
        base.owning_root_community_id::text AS owning_root_community_id,
        NULLIF({{ clean_text('base.owning_root_community_title') }}, '')::text AS owning_root_community_title,
        NULLIF({{ clean_text('base.owning_community_path_titles') }}, '')::text AS owning_community_path_titles,
        base.owning_community_id::text AS owning_community_id,
        NULLIF({{ clean_text('base.owning_community_title') }}, '')::text AS owning_community_title,
        base.owning_collection::text AS owning_collection,
        NULLIF({{ clean_text('base.owning_collection_title') }}, '')::text AS owning_collection_title,
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
    LEFT JOIN {{ ref('dim_unlp_sedici_item_type') }} AS type_map
        ON type_map.source = 'sedici'
       AND LOWER(BTRIM(type_map.type)) = LOWER(BTRIM(base.type_raw))
       AND COALESCE(LOWER(BTRIM(type_map.subtype)), '') = COALESCE(LOWER(BTRIM(base.subtype_raw)), '')
),

final AS (
    SELECT
        source,
        id,
        type,
        subtype,
        date,
        dc_date_available,
        title,
        subtitle,
        author,
        doi,
        issn,
        isbn,
        subject,
        owning_root_community_id,
        owning_root_community_title,
        owning_community_path_titles,
        owning_community_id,
        owning_community_title,
        owning_collection,
        owning_collection_title,
        publication_year,
        type_dedup,
        description,
        (
            title IS NOT NULL
            AND type_dedup IS NOT NULL
            AND author IS NOT NULL
            AND publication_year IS NOT NULL
        ) AS dedup_eligible
    FROM prepared
)

SELECT *
FROM final
