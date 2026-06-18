{{ config(materialized='table') }}

WITH base AS (
    SELECT
        sat.community_hk,
        sat.community_id,
        sat.community_uuid,
        extract.base_url,
        extract.source_label,
        extract.institution_ror
    FROM {{ ref('latest_sat_dspacedb_community') }} AS sat
    INNER JOIN (
        SELECT DISTINCT
            community_hk,
            base_url,
            source_label,
            institution_ror
        FROM {{ ref('stg_dspacedb_community') }}
    ) AS extract
        USING (community_hk, base_url, source_label, institution_ror)
),

community_title AS (
    SELECT
        base.community_hk,
        MIN(mv.text_value) AS community_title
    FROM base
    INNER JOIN {{ ref('latest_sat_dspacedb_metadatavalue') }} AS mv
        ON mv.dspace_object_id = base.community_uuid
       AND mv.base_url = base.base_url
       AND mv.institution_ror = base.institution_ror
    INNER JOIN {{ ref('dim_dspacedb_metadatafield') }} AS mf
        ON mv.metadatafield_hk = mf.metadatafield_hk
       AND mv.base_url = mf.base_url
       AND mv.institution_ror = mf.institution_ror
    WHERE mf.metadatafield_fullname = 'dc.title'
      AND mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
    GROUP BY base.community_hk
),

final AS (
    SELECT
        base.community_hk,
        base.community_id,
        base.community_uuid,
        title.community_title,
        REGEXP_REPLACE(base.base_url, '/+$', '') || '/communities/' || base.community_uuid || '/' AS community_url,
        base.base_url,
        base.source_label,
        base.institution_ror
    FROM base
    LEFT JOIN community_title AS title
        USING (community_hk)
)

SELECT * FROM final
