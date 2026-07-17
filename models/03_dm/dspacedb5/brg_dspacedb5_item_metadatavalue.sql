{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_hk', 'metadatavalue_hk'], 'unique': true},
        {'columns': ['item_hk', 'metadatafield_fullname']},
        {'columns': ['item_id', 'institution_ror', 'source_label', 'base_url', 'metadatafield_fullname']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH resource_metadatavalue AS (
    SELECT
        item_hk,
        metadatavalue_hk,
        metadatafield_hk
    FROM {{ ref('latest_sat_dspacedb5_item_metadatavalue') }}
),
item_scope AS (
    SELECT
        item_hk,
        SPLIT_PART(item_bk, '||', 1) AS institution_ror,
        SPLIT_PART(item_bk, '||', 2) AS source_label,
        SPLIT_PART(item_bk, '||', 3) AS base_url,
        SPLIT_PART(item_bk, '||', 4)::bigint AS item_id
    FROM {{ ref('hub_dspacedb5_item') }}
),
metadatavalue_scope AS (
    SELECT
        hub_mv.metadatavalue_hk,
        SPLIT_PART(hub_mv.metadatavalue_bk, '||', 4)::bigint AS metadata_value_id,
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        sat_mv.load_datetime
    FROM {{ ref('hub_dspacedb5_metadatavalue') }} AS hub_mv
    JOIN {{ latest_satellite(ref('sat_dspacedb5_metadatavalue'), 'metadatavalue_hk', order_column='load_datetime') }} AS sat_mv
        USING (metadatavalue_hk)
),
metadatafield_scope AS (
    SELECT
        hub_mf.metadatafield_hk,
        SPLIT_PART(hub_mf.metadatafield_bk, '||', 4)::bigint AS metadata_field_id,
        mf.metadatafield_fullname,
        mf.short_id,
        mf.element,
        mf.qualifier
    FROM {{ ref('hub_dspacedb5_metadatafieldregistry') }} AS hub_mf
    JOIN {{ ref('dim_dspacedb5_metadatafield') }} AS mf
        USING (metadatafield_hk)
),
relation_scope AS (
    SELECT
        item_hk,
        metadatavalue_hk,
        metadatafield_hk
    FROM resource_metadatavalue
),
final AS (
    SELECT
        rel.item_hk,
        item.item_id,
        item.source_label,
        item.institution_ror,
        item.base_url,
        rel.metadatavalue_hk,
        rel.metadatafield_hk,
        mv.metadata_value_id,
        mf.metadata_field_id,
        mf.metadatafield_fullname,
        mf.short_id,
        mf.element,
        mf.qualifier,
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        mv.load_datetime
    FROM relation_scope AS rel
    JOIN item_scope AS item
        USING (item_hk)
    JOIN metadatavalue_scope AS mv
        USING (metadatavalue_hk)
    JOIN metadatafield_scope AS mf
        USING (metadatafield_hk)
)

SELECT * FROM final
