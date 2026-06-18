{{ config(materialized='table') }}

WITH collection_base AS (
    SELECT
        collection_hk,
        collection_bk,
        collection_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('stg_dspacedb5_collection') }}
),
title_candidates AS (
    SELECT
        c.collection_hk,
        mv.text_value AS collection_title,
        ROW_NUMBER() OVER (
            PARTITION BY c.collection_hk
            ORDER BY
                (mv.text_lang IS NULL) DESC,
                mv.text_lang,
                mv.place,
                mv.text_value,
                mv.metadatavalue_hk
        ) AS rn
    FROM collection_base c
    JOIN {{ ref('brg_dspacedb5_collection_metadatavalue') }} mv
        USING (collection_hk)
    WHERE mv.short_id = 'dc'
      AND mv.element = 'title'
      AND COALESCE(mv.qualifier, '') IN ('', '!UNKNOWN')
      AND mv.source_label = c.source_label
      AND mv.institution_ror = c.institution_ror
      AND mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
),
final AS (
    SELECT
        c.collection_hk,
        c.collection_id,
        tc.collection_title,
        c.source_label,
        c.institution_ror,
        c.extract_datetime,
        c.load_datetime
    FROM collection_base c
    LEFT JOIN title_candidates tc
        ON c.collection_hk = tc.collection_hk
       AND tc.rn = 1
)

SELECT * FROM final
