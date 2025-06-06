{{ config(materialized = 'table') }}

WITH item AS (
    SELECT 
        hub_i.item_id,
        sat_i.in_archive,
        sat_i.withdrawn,
        sat_i.discoverable,
        hub_i.item_hk
    FROM {{ref('hub_dspace5_item')}} hub_i
    INNER JOIN {{ref('sat_dspace5_item')}} sat_i 
        ON sat_i.item_hk = hub_i.item_hk    
    WHERE sat_i.in_archive = True AND sat_i.withdrawn = False
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
        (ms.short_id = 'dc'
        AND (
            (mf.element = 'identifier' AND mf.qualifier = 'uri') OR
            (mf.element = 'date' AND mf.qualifier = 'accessioned') OR
            (mf.element = 'date' AND mf.qualifier = 'issued') OR
            (mf.element = 'title' AND mf.qualifier IS NULL) OR
            (mf.element = 'type' AND mf.qualifier IS NULL)
        ))
        OR
        (ms.short_id = 'sedici'
        AND (
            (mf.element = 'subtype' AND mf.qualifier is null)
        )) 
),

-- Valores asociados
metadatavalues AS (
    SELECT 
        mf.element,
        mf.qualifier,
        mv.text_value,
        lnk_mv_r.resource_hk AS item_hk
    FROM metadata_fields mf
    INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf 
        ON lnk_mv_mf.metadatafield_hk = mf.metadatafield_hk
    INNER JOIN {{ref('link_dspace5_metadatavalue_resource')}} lnk_mv_r 
        ON lnk_mv_r.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    INNER JOIN {{ref('sat_dspace5_metadatavalue')}} mv 
        ON mv.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    WHERE mv.resource_type_id = 2
),

-- Pivot de metadatos por item
item_metadata AS (
    SELECT 
        item_hk,
        MAX(CASE WHEN element = 'identifier' AND qualifier = 'uri' THEN text_value END) AS uri,
        MAX(CASE WHEN element = 'date' AND qualifier = 'accessioned' THEN text_value END) AS date_accessioned,
        MAX(CASE WHEN element = 'date' AND qualifier = 'issued' THEN text_value END) AS date_issued,
        MAX(CASE WHEN element = 'title' AND qualifier IS NULL THEN text_value END) AS title,
        MAX(CASE WHEN element = 'type' AND qualifier IS NULL THEN text_value END) AS type,
        MAX(CASE WHEN element = 'subtype' AND qualifier IS NULL THEN text_value END) AS subtype
    FROM metadatavalues
    GROUP BY item_hk
),

fct_item AS (
    -- Unión final
    SELECT 
        i.item_id,
        i.in_archive,
        i.withdrawn,
        i.discoverable,
        m.uri,
        m.date_accessioned,
        m.date_issued,
        m.title,
        m.type,
        m.subtype,
        i.item_hk
    FROM item i
    LEFT JOIN item_metadata m 
        ON i.item_hk = m.item_hk
),

fct_item_col AS (
    SELECT 
        fct_item.item_id,
        fct_item.in_archive,
        fct_item.withdrawn,
        fct_item.discoverable,
        fct_item.uri,
        fct_item.date_accessioned,
        fct_item.date_issued,
        fct_item.title,
        fct_item.type,
        fct_item.subtype,
        fct_item.item_hk,
        lnk_i_col.owningcollection_hk
    FROM fct_item
    INNER JOIN {{ref('link_dspace5_item_owningcollection')}} lnk_i_col ON
        fct_item.item_hk = lnk_i_col.item_hk
)

SELECT * FROM fct_item_col