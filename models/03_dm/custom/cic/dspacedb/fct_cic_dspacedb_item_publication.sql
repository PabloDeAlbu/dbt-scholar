{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb_item_publication') }}
    WHERE institution_ror = 'https://ror.org/02s7sax82'
),

owning_collection AS (
    SELECT
        base.item_uuid,
        MIN(col.collection_title) AS owning_collection_title
    FROM base
    LEFT JOIN {{ ref('dim_dspacedb_collection') }} AS col
        ON base.owning_collection = col.collection_uuid
       AND base.source_label = col.source_label
       AND base.institution_ror = col.institution_ror
    GROUP BY base.item_uuid
),

metadatafield_title AS (
    SELECT
        metadatafield_hk
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE short_id = 'dc'
      AND element = 'title'
      AND COALESCE(qualifier, '') IN ('', '!UNKNOWN')
    GROUP BY 1
),

title_agg AS (
    SELECT
        base.item_uuid,
        MIN(mv.text_value) AS dc_title,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS dc_title_raw
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
    JOIN metadatafield_title AS mft
        ON mv.metadatafield_hk = mft.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
    GROUP BY base.item_uuid
),

final AS (
    SELECT
        base.*,
        owning.owning_collection_title,
        title.dc_title,
        title.dc_title_raw
    FROM base
    LEFT JOIN owning_collection AS owning
        USING (item_uuid)
    LEFT JOIN title_agg AS title
        USING (item_uuid)
)

SELECT * FROM final
