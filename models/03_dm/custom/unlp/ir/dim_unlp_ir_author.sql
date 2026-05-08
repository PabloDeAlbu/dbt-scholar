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

aggregated AS (
    SELECT
        author_bk,
        MIN(author_name_raw) AS author_name_preferred,
        MIN(author_name_normalized) AS author_name_normalized,
        MIN(author_type) AS author_type,
        MIN(authority) AS authority,
        BOOL_OR(authority IS NOT NULL) AS has_authority_control,
        MIN(confidence) AS min_confidence,
        MAX(confidence) AS max_confidence,
        COUNT(DISTINCT author_name_raw)::integer AS observed_name_variant_count,
        COUNT(DISTINCT item_hk)::integer AS item_count,
        MIN(source_label) AS source_label,
        MIN(institution_ror) AS institution_ror
    FROM normalized
    GROUP BY author_bk
),

final AS (
    SELECT
        {{ automate_dv.hash(columns='author_bk', alias='ir_author_hk') }},
        author_bk,
        author_name_preferred,
        author_name_normalized,
        author_type,
        authority,
        has_authority_control,
        min_confidence,
        max_confidence,
        observed_name_variant_count,
        item_count,
        source_label,
        institution_ror
    FROM aggregated
)

SELECT * FROM final
