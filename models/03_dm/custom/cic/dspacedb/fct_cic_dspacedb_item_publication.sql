{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb_item_publication') }}
    WHERE institution_ror IN (
        SELECT institution_ror
        FROM {{ ref('seed_dspacedb_repository') }}
        WHERE institution_key = 'cic'
    )
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
        title.dc_title,
        title.dc_title_raw
    FROM base
    LEFT JOIN title_agg AS title
        USING (item_uuid)
)

SELECT * FROM final
