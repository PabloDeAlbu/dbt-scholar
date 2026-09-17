{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': true},
        {'columns': ['publication_year']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
    WHERE in_archive IS TRUE
      AND withdrawn IS FALSE
      AND discoverable IS TRUE
      AND handle IS NOT NULL
),

type_seed AS (
    SELECT DISTINCT ON (LOWER(BTRIM(type)))
        LOWER(BTRIM(type)) AS type_key,
        coar_uri
    FROM {{ ref('seed_sedici-types2coar-types') }}
    WHERE NULLIF(BTRIM(type), '') IS NOT NULL
    ORDER BY LOWER(BTRIM(type)), coar_uri
),

author_ranked AS (
    SELECT
        item_id,
        author_name,
        CASE author_role
            WHEN 'creator_person' THEN 1
            WHEN 'creator_corporate' THEN 2
            WHEN 'contributor_compiler' THEN 3
        END AS author_role_order,
        author_place,
        metadata_value_id,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, LOWER(author_name)
            ORDER BY
                CASE author_role
                    WHEN 'creator_person' THEN 1
                    WHEN 'creator_corporate' THEN 2
                    WHEN 'contributor_compiler' THEN 3
                END,
                author_place NULLS LAST,
                metadata_value_id
        ) AS author_rank
    FROM {{ ref('brg_unlp_sedicidb_item_author') }}
),

author_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            author_name,
            '|'
            ORDER BY
                author_role_order,
                author_place NULLS LAST,
                metadata_value_id,
                author_name
        ) AS author
    FROM author_ranked
    WHERE author_rank = 1
    GROUP BY item_id
),

metadatafield AS (
    SELECT
        field.metadata_field_id,
        schema_registry.short_id || '.' || field.element || CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN '.' || BTRIM(field.qualifier)
            ELSE ''
        END AS metadatafield_fullname
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

metadata_value AS (
    SELECT
        base.item_id,
        value.metadata_value_id,
        field.metadatafield_fullname,
        NULLIF(REPLACE({{ clean_text('value.text_value') }}, '|', ' '), '') AS text_value_clean,
        value.place
    FROM base
    INNER JOIN {{ source('sedicidb', 'metadatavalue') }} AS value
        ON value.resource_id = base.item_id
       AND value.resource_type_id = 2
    INNER JOIN metadatafield AS field
        USING (metadata_field_id)
    WHERE field.metadatafield_fullname IN (
        'dc.title',
        'dc.title.alternative',
        'sedici.title.subtitle',
        'dc.subject',
        'sedici.subject.materias',
        'dc.description',
        'dc.description.abstract',
        'dc.identifier',
        'dc.identifier.isbn',
        'dc.identifier.issn',
        'dc.identifier.other',
        'dc.identifier.uri',
        'sedici.identifier.doi',
        'sedici.identifier.isbn',
        'sedici.identifier.issn',
        'sedici.identifier.other',
        'sedici.identifier.uri'
    )
),

ranked_value AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, metadatafield_fullname
            ORDER BY place NULLS LAST, metadata_value_id
        ) AS value_rank
    FROM metadata_value
    WHERE text_value_clean IS NOT NULL
),

list_value AS (
    SELECT
        item_id,
        CASE
            WHEN metadatafield_fullname IN ('dc.title', 'dc.title.alternative') THEN 'title'
            WHEN metadatafield_fullname = 'sedici.title.subtitle' THEN 'subtitle'
            WHEN metadatafield_fullname IN ('sedici.subject.materias', 'dc.subject') THEN 'subject'
            WHEN metadatafield_fullname IN ('dc.description.abstract', 'dc.description') THEN 'description'
            WHEN metadatafield_fullname IN ('sedici.identifier.isbn', 'dc.identifier.isbn') THEN 'isbn'
            WHEN metadatafield_fullname IN ('sedici.identifier.issn', 'dc.identifier.issn') THEN 'issn'
        END AS list_name,
        CASE metadatafield_fullname
            WHEN 'dc.title' THEN 1
            WHEN 'dc.title.alternative' THEN 2
            WHEN 'sedici.title.subtitle' THEN 1
            WHEN 'sedici.subject.materias' THEN 1
            WHEN 'dc.subject' THEN 2
            WHEN 'dc.description.abstract' THEN 1
            WHEN 'dc.description' THEN 2
            WHEN 'sedici.identifier.isbn' THEN 1
            WHEN 'dc.identifier.isbn' THEN 2
            WHEN 'sedici.identifier.issn' THEN 1
            WHEN 'dc.identifier.issn' THEN 2
        END AS source_order,
        text_value_clean,
        place
    FROM ranked_value
    WHERE metadatafield_fullname IN (
        'dc.title',
        'sedici.title.subtitle',
        'dc.title.alternative',
        'sedici.subject.materias',
        'dc.subject',
        'dc.description.abstract',
        'dc.description',
        'sedici.identifier.isbn',
        'dc.identifier.isbn',
        'sedici.identifier.issn',
        'dc.identifier.issn'
    )
),

list_value_distinct AS (
    SELECT
        item_id,
        list_name,
        LOWER(text_value_clean) AS value_key,
        MIN(text_value_clean) AS text_value_clean,
        MIN(source_order) AS source_order,
        MIN(place) AS first_place
    FROM list_value
    GROUP BY item_id, list_name, LOWER(text_value_clean)
),

list_agg AS (
    SELECT
        item_id,
        list_name,
        STRING_AGG(
            text_value_clean,
            '|'
            ORDER BY source_order, first_place NULLS LAST, text_value_clean
        ) AS text_values
    FROM list_value_distinct
    GROUP BY item_id, list_name
),

list_metadata AS (
    SELECT
        item_id,
        MAX(text_values) FILTER (WHERE list_name = 'title') AS title,
        MAX(text_values) FILTER (WHERE list_name = 'subtitle') AS subtitle,
        MAX(text_values) FILTER (WHERE list_name = 'subject') AS subject,
        MAX(text_values) FILTER (WHERE list_name = 'description') AS description,
        MAX(text_values) FILTER (WHERE list_name = 'isbn') AS isbn,
        MAX(text_values) FILTER (WHERE list_name = 'issn') AS issn
    FROM list_agg
    GROUP BY item_id
),

doi_value AS (
    SELECT DISTINCT
        item_id,
        REGEXP_REPLACE(
            SUBSTRING(
                LOWER(text_value_clean)
                FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'
            ),
            '[.),;:]+$',
            ''
        ) AS doi
    FROM ranked_value
    WHERE metadatafield_fullname IN (
        'dc.identifier',
        'dc.identifier.other',
        'dc.identifier.uri',
        'sedici.identifier.doi',
        'sedici.identifier.other',
        'sedici.identifier.uri'
    )
      AND SUBSTRING(
          LOWER(text_value_clean)
          FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'
      ) IS NOT NULL
),

doi_agg AS (
    SELECT
        item_id,
        STRING_AGG(doi, '|' ORDER BY doi) AS doi
    FROM doi_value
    GROUP BY item_id
),

prepared AS (
    SELECT
        'sedici'::text AS source,
        base.handle::text AS id,
        CASE
            WHEN LOWER(BTRIM(base.sedici_subtype)) = 'capitulo de libro' THEN 'bookpart'
            WHEN LOWER(BTRIM(base.dc_type)) IN ('articulo', 'artículo') THEN 'article'
            WHEN LOWER(BTRIM(base.dc_type)) = 'tesis' THEN 'tesis'
            WHEN LOWER(BTRIM(base.dc_type)) = 'libro' THEN 'book'
            WHEN LOWER(BTRIM(base.dc_type)) = 'objeto de conferencia' THEN 'objeto de conferencia'
            ELSE 'unknown'
        END::text AS type,
        base.dc_type::text AS type_raw,
        base.sedici_subtype::text AS subtype,
        CASE
            WHEN base.dc_date_issued BETWEEN '1500-01-01'::date AND '2100-12-31'::date
                THEN base.dc_date_issued
            WHEN base.sedici_date_exposure BETWEEN '1500-01-01'::date AND '2100-12-31'::date
                THEN base.sedici_date_exposure
        END AS date,
        base.dc_date_available,
        lists.title::text AS title,
        lists.subtitle::text AS subtitle,
        authors.author::text AS author,
        doi.doi::text AS doi,
        lists.issn::text AS issn,
        lists.isbn::text AS isbn,
        lists.subject::text AS subject,
        base.owning_root_community_id::text AS owning_root_community_id,
        base.owning_root_community_title::text AS owning_root_community_title,
        base.owning_community_path_titles::text AS owning_community_path_titles,
        base.owning_community_id::text AS owning_community_id,
        base.owning_community_title::text AS owning_community_title,
        base.owning_collection::text AS owning_collection,
        base.owning_collection_title::text AS owning_collection_title,
        CASE
            WHEN base.dc_date_issued BETWEEN '1500-01-01'::date AND '2100-12-31'::date
                THEN EXTRACT(YEAR FROM base.dc_date_issued)::integer
        END AS publication_year,
        CASE
            WHEN type_seed.coar_uri IS NULL THEN NULL
            WHEN coar.label IN ('JOURNAL ARTICLE', 'RESEARCH ARTICLE') THEN 'ARTÍCULO'
            WHEN coar.label = 'DOCTORAL THESIS' THEN 'TESIS'
            WHEN coar.label = 'THESIS' OR coar.parent_label_1 = 'THESIS' THEN 'TESIS'
            ELSE coar.label_es
        END::text AS type_dedup,
        lists.description::text AS description
    FROM base
    LEFT JOIN author_agg AS authors
        USING (item_id)
    LEFT JOIN list_metadata AS lists
        USING (item_id)
    LEFT JOIN doi_agg AS doi
        USING (item_id)
    LEFT JOIN type_seed
        ON type_seed.type_key = LOWER(BTRIM(base.dc_type))
    LEFT JOIN {{ ref('dim_coar_resource_type') }} AS coar
        ON coar.coar_uri = type_seed.coar_uri
)

SELECT *
FROM prepared
