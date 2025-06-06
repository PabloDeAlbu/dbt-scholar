{{ config(materialized = 'table') }}

WITH col AS (
    SELECT 
        hub_c.collection_hk,
        sat_c.collection_id
    FROM {{ref('hub_dspace5_collection')}} hub_c
    INNER JOIN {{ref('sat_dspace5_collection')}} sat_c
        ON sat_c.collection_hk = hub_c.collection_hk
),

-- Metacampos de interés
metadata_fields AS (
    SELECT 
        mf.metadatafield_hk,
        ms.short_id AS schema,
        mf.element,
        mf.qualifier
    FROM {{ref('sat_dspace5_metadatafieldregistry')}} mf
    INNER JOIN {{ref('link_dspace5_metadatafield_metadataschema')}} lnk 
        ON lnk.metadatafield_hk = mf.metadatafield_hk
    INNER JOIN {{ref('sat_dspace5_metadataschemaregistry')}} ms 
        ON ms.metadataschema_hk = lnk.metadataschema_hk
    WHERE 
        ms.short_id = 'dc'
        AND (
            (mf.element = 'title' AND mf.qualifier IS NULL)
        )
),

-- Valores asociados
metadatavalues AS (
    SELECT 
        mf.element,
        mf.qualifier,
        mv.text_value,
        lnk_mv_r.resource_hk AS collection_hk
    FROM metadata_fields mf
    INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf 
        ON lnk_mv_mf.metadatafield_hk = mf.metadatafield_hk
    INNER JOIN {{ref('link_dspace5_metadatavalue_resource')}} lnk_mv_r 
        ON lnk_mv_r.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    INNER JOIN {{ref('sat_dspace5_metadatavalue')}} mv 
        ON mv.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    WHERE mv.resource_type_id = 3
),

-- Pivot de metadatos por col
col_metadata AS (
    SELECT 
        collection_hk,
        MAX(CASE WHEN element = 'title' AND qualifier IS NULL THEN text_value END) AS title
    FROM metadatavalues
    GROUP BY collection_hk
),

dim_collection AS (
    -- Unión final
    SELECT 
        c.collection_id,
        m.title,
        sat_h.handle,
        c.collection_hk
    FROM col c
    LEFT JOIN col_metadata m 
        ON c.collection_hk = m.collection_hk
    INNER JOIN {{ref('link_dspace5_handle_resource')}} lnk_h_r ON
        lnk_h_r.resource_hk = c.collection_hk
    INNER JOIN {{ref('sat_dspace5_handle')}} sat_h ON
        sat_h.handle_hk = lnk_h_r.handle_hk
    WHERE sat_h.resource_type_id = 3
)

SELECT * FROM dim_collection