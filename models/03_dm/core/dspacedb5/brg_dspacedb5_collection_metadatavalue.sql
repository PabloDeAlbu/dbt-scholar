{{ config(materialized='table') }}

WITH resource_metadatavalue AS (
    SELECT DISTINCT
        tl.metadatavalue_hk,
        tl.resource_hk AS collection_hk
    FROM {{ ref('tlink_dspacedb5_metadatavalue_resource') }} tl
    WHERE tl.resource_type_id = 3
),
collection_metadata AS (
    SELECT
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        rm.metadatavalue_hk,
        rm.collection_hk
    FROM resource_metadatavalue rm
    JOIN {{ ref('dim_dspacedb5_metadatavalue') }} mv
        USING (metadatavalue_hk)
),
final AS (
    SELECT
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        mf.short_id,
        mf.element,
        mf.qualifier,
        mf.metadatafield_fullname,
        mf.metadatafield_hk,
        mf.source_label,
        mf.institution_ror,
        mv.metadatavalue_hk,
        mv.collection_hk
    FROM collection_metadata mv
    JOIN {{ ref('link_dspacedb5_metadatavalue_metadatafield') }} lnk
        USING (metadatavalue_hk)
    JOIN {{ ref('dim_dspacedb5_metadatafield') }} mf
        USING (metadatafield_hk)
)

SELECT * FROM final
