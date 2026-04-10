{{ config(materialized='table') }}

WITH base AS (
    SELECT
        sat.collection_hk,
        sat.collection_id,
        sat.collection_uuid,
        extract.base_url,
        extract.source_label,
        extract.institution_ror
    FROM {{ ref('latest_sat_dspacedb_collection') }} AS sat
    INNER JOIN {{ ref('latest_sat_dspacedb_collection__extract') }} AS extract
        USING (collection_hk)
),

collection_title AS (
    SELECT
        base.collection_hk,
        MIN(mv.text_value) AS collection_title
    FROM base
    INNER JOIN {{ ref('latest_sat_dspacedb_metadatavalue') }} AS mv
        ON mv.dspace_object_id = base.collection_uuid
       AND mv.base_url = base.base_url
       AND mv.institution_ror = base.institution_ror
    INNER JOIN {{ ref('dim_dspacedb_metadatafield') }} AS mf
        ON mv.metadatafield_hk = mf.metadatafield_hk
       AND mv.base_url = mf.base_url
       AND mv.institution_ror = mf.institution_ror
    WHERE mf.metadatafield_fullname = 'dc.title'
      AND mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
    GROUP BY base.collection_hk
),

final AS (
    SELECT
        base.collection_hk,
        base.collection_id,
        base.collection_uuid,
        title.collection_title,
        base.base_url,
        base.source_label,
        base.institution_ror
    FROM base
    LEFT JOIN collection_title AS title
        USING (collection_hk)
)

SELECT * FROM final
