{{ config(
    materialized='table',
    indexes=[
        {'columns': ['publication_type_id'], 'unique': true},
        {'columns': ['type_raw', 'subtype']},
        {'columns': ['type']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH ranked_value AS (
    SELECT
        item_id,
        metadatafield_fullname,
        NULLIF(REPLACE(text_value, '|', ' '), '') AS text_value,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, metadatafield_fullname
            ORDER BY place NULLS LAST, metadata_value_id
        ) AS value_rank
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN ('dc.type', 'sedici.subtype')
      AND text_value IS NOT NULL
),

item_type AS (
    SELECT
        item_id,
        MAX(text_value) FILTER (
            WHERE metadatafield_fullname = 'dc.type' AND value_rank = 1
        ) AS type_raw,
        MAX(text_value) FILTER (
            WHERE metadatafield_fullname = 'sedici.subtype' AND value_rank = 1
        ) AS subtype
    FROM ranked_value
    GROUP BY item_id
),

observed_type AS (
    SELECT DISTINCT
        type_raw,
        subtype
    FROM item_type
    WHERE type_raw IS NOT NULL
),

type_seed AS (
    SELECT DISTINCT ON (LOWER(BTRIM(type)))
        LOWER(BTRIM(type)) AS type_key,
        coar_uri
    FROM {{ ref('seed_sedici-types2coar-types') }}
    WHERE NULLIF(BTRIM(type), '') IS NOT NULL
    ORDER BY LOWER(BTRIM(type)), coar_uri
),

final AS (
    SELECT
        MD5(
            observed.type_raw
            || '||'
            || COALESCE(observed.subtype, '')
        ) AS publication_type_id,
        observed.type_raw,
        observed.subtype,
        CASE
            WHEN LOWER(BTRIM(observed.subtype)) = 'capitulo de libro' THEN 'bookpart'
            WHEN LOWER(BTRIM(observed.type_raw)) IN ('articulo', 'artículo') THEN 'article'
            WHEN LOWER(BTRIM(observed.type_raw)) = 'tesis' THEN 'tesis'
            WHEN LOWER(BTRIM(observed.type_raw)) = 'libro' THEN 'book'
            WHEN LOWER(BTRIM(observed.type_raw)) = 'objeto de conferencia' THEN 'objeto de conferencia'
            ELSE 'unknown'
        END::text AS type,
        seed.coar_uri,
        CASE
            WHEN seed.coar_uri IS NULL THEN NULL
            WHEN coar.label IN ('JOURNAL ARTICLE', 'RESEARCH ARTICLE') THEN 'ARTÍCULO'
            WHEN coar.label = 'DOCTORAL THESIS' THEN 'TESIS'
            WHEN coar.label = 'THESIS' OR coar.parent_label_1 = 'THESIS' THEN 'TESIS'
            ELSE coar.label_es
        END AS type_dedup
    FROM observed_type AS observed
    LEFT JOIN type_seed AS seed
        ON seed.type_key = LOWER(BTRIM(observed.type_raw))
    LEFT JOIN {{ ref('dim_coar_resource_type') }} AS coar
        ON coar.coar_uri = seed.coar_uri
)

SELECT *
FROM final
