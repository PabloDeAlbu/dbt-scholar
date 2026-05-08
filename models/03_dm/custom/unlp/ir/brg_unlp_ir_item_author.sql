{{ config(materialized='table') }}

WITH author_observation AS (
    SELECT
        item.item_hk,
        item.item_id,
        item.source_label,
        item.institution_ror,
        metadata.metadata_value_id,
        metadata.metadatafield_fullname,
        metadata.text_value AS author_name_raw,
        metadata.authority,
        metadata.confidence,
        metadata.place
    FROM {{ ref('fct_unlp_ir_item_publication') }} AS item
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS metadata
        USING (item_hk)
    WHERE metadata.metadatafield_fullname IN ('sedici.creator.person', 'sedici.creator.corporate')
      AND NULLIF(TRIM(metadata.text_value), '') IS NOT NULL
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
        {{ automate_dv.hash(columns='author_bk', alias='ir_author_hk') }},
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

SELECT * FROM final
