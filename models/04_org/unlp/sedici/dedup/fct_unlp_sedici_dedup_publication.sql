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
        sedici_creator_person,
        sedici_creator_corporate,
        sedici_contributor_compiler,
        doi,
        isbn,
        issn,
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
        NULLIF(
            CONCAT_WS(
                '|',
                NULLIF({{ clean_text('base.sedici_creator_person') }}, ''),
                NULLIF({{ clean_text('base.sedici_creator_corporate') }}, ''),
                NULLIF({{ clean_text('base.sedici_contributor_compiler') }}, '')
            ),
            ''
        )::text AS author,
        base.doi::text AS doi,
        base.issn::text AS issn,
        base.isbn::text AS isbn,
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
