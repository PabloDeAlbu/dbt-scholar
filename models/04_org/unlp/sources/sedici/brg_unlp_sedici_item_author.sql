{{ config(materialized='table') }}

WITH metadata_base AS (
    SELECT
        item_hk,
        metadata_value_id,
        metadatafield_fullname,
        text_value AS author_name_raw,
        authority,
        confidence,
        place
    FROM {{ ref('brg_dspacedb5_item_metadatavalue') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
      AND metadatafield_fullname IN ('sedici.creator.person', 'sedici.creator.corporate')
      AND NULLIF(TRIM(text_value), '') IS NOT NULL
),

item_scope AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror
    FROM {{ ref('fct_unlp_sedici_item_publication') }}
),

author_observation AS (
    SELECT
        item_scope.item_hk,
        item_scope.item_id,
        item_scope.source_label,
        item_scope.institution_ror,
        metadata.metadata_value_id,
        metadata.metadatafield_fullname,
        metadata.author_name_raw,
        metadata.authority,
        metadata.confidence,
        metadata.place
    FROM metadata_base AS metadata
    INNER JOIN item_scope
        USING (item_hk)
),

normalized AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        metadata_value_id,
        metadatafield_fullname,
        author_name_raw,
        NULLIF(TRIM(authority), '') AS authority,
        confidence,
        place,
        CASE
            WHEN metadatafield_fullname = 'sedici.creator.person' THEN 'person'
            WHEN metadatafield_fullname = 'sedici.creator.corporate' THEN 'corporate'
            ELSE 'unknown'
        END AS author_type,
        LOWER(
            REGEXP_REPLACE(
                TRIM(author_name_raw),
                '\s+',
                ' ',
                'g'
            )
        ) AS author_name_normalized,
        CASE
            WHEN NULLIF(TRIM(authority), '') IS NOT NULL THEN
                institution_ror || '||authority||' || LOWER(TRIM(authority))
            ELSE
                institution_ror || '||name||' || metadatafield_fullname || '||' || LOWER(
                    REGEXP_REPLACE(
                        TRIM(author_name_raw),
                        '\s+',
                        ' ',
                        'g'
                    )
                )
        END AS author_bk
    FROM author_observation
),

final AS (
    SELECT
        item_hk,
        item_id,
        {{ automate_dv.hash(columns='author_bk', alias='sedici_author_hk') }},
        metadata_value_id,
        metadatafield_fullname,
        author_type,
        author_name_raw,
        author_name_normalized,
        authority,
        (authority IS NOT NULL) AS has_authority_control,
        confidence,
        place AS author_place,
        source_label,
        institution_ror
    FROM normalized
)

SELECT *
FROM final
