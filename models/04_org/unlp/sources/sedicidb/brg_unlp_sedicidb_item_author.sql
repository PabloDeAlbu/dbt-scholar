{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_value_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['author_id']},
        {'columns': ['authority']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- ¿Qué observaciones representan autorías?
WITH author_observation AS (
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

-- ¿Cómo se identifica cada autoría?
identified AS (
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
        author_place,
        CASE
            WHEN authority IS NOT NULL
                THEN 'authority||' || authority
            ELSE 'name||' || author_type || '||' || LOWER(author_name)
        END AS author_bk
    FROM author_observation
    WHERE author_name IS NOT NULL
),

-- ¿Qué clave técnica tiene cada autoría?
final AS (
    SELECT
        MD5(author_bk) AS author_id,
        *
    FROM identified
)

SELECT *
FROM final
