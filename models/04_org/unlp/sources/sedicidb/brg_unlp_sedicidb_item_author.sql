{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_value_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['authority']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- ¿Qué valores de metadatos de ítem corresponden a autores o compiladores?
WITH item_author_metadatavalue AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        CASE metadatafield_fullname
            WHEN 'sedici.creator.person' THEN 'creator_person'
            WHEN 'sedici.creator.corporate' THEN 'creator_corporate'
            WHEN 'sedici.contributor.compiler' THEN 'contributor_compiler'
        END AS author_role,
        CASE
            WHEN metadatafield_fullname = 'sedici.creator.corporate' THEN 'corporate'
            ELSE 'person'
        END AS author_type,
        text_value_raw AS author_name_raw,
        NULLIF(
            TRIM(REGEXP_REPLACE(REPLACE(text_value, '|', ' '), '\s+', ' ', 'g')),
            ''
        ) AS author_name,
        authority_raw,
        authority,
        confidence,
        place AS author_place
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname IN (
        'sedici.creator.person',
        'sedici.creator.corporate',
        'sedici.contributor.compiler'
    )
),

-- ¿Qué ítems se relacionan con qué autores o compiladores?
final AS (
    SELECT
        item_id,
        metadata_value_id,
        metadatafield_fullname,
        author_role,
        author_type,
        author_name_raw,
        author_name,
        authority_raw,
        authority,
        authority IS NOT NULL AS has_authority_control,
        confidence,
        author_place
    FROM item_author_metadatavalue
    WHERE author_name IS NOT NULL
)

SELECT *
FROM final
