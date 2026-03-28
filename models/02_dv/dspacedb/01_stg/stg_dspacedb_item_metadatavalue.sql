{{ config(materialized='view') }}

WITH item AS (
    SELECT
        item_hk,
        item_bk,
        item_id,
        item_uuid,
        source_label,
        institution_ror
    FROM {{ ref('stg_dspacedb_item') }}
),
metadatafield AS (
    SELECT
        mf.metadatafield_hk,
        mf.metadatafield_bk,
        mf.metadata_field_id,
        ms.short_id,
        mf.element,
        mf.qualifier,
        CASE
            WHEN mf.qualifier IS NOT NULL AND mf.qualifier <> '!UNKNOWN'
                THEN ms.short_id || '.' || mf.element || '.' || mf.qualifier
            ELSE ms.short_id || '.' || mf.element
        END AS metadatafield_fullname
    FROM {{ ref('stg_dspacedb_metadatafieldregistry') }} AS mf
    JOIN {{ ref('stg_dspacedb_metadataschemaregistry') }} AS ms
        USING (metadataschema_hk)
),
raw_source AS (
    SELECT
        it.item_hk,
        it.item_bk,
        it.item_id,
        it.item_uuid,
        mv.metadatavalue_hk,
        mv.metadatavalue_bk,
        mf.metadatafield_hk,
        mf.metadatafield_bk,
        mv.metadata_value_id,
        mv.metadata_field_id,
        mf.short_id,
        mf.element,
        mf.qualifier,
        mf.metadatafield_fullname,
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        mv.extract_datetime,
        mv.load_datetime,
        it.source_label,
        it.institution_ror
    FROM {{ ref('stg_dspacedb_metadatavalue') }} AS mv
    JOIN item AS it
        ON mv.dspace_object_id = it.item_uuid
    JOIN metadatafield AS mf
        ON mv.metadatafield_hk = mf.metadatafield_hk
),
final AS (
    SELECT
        item_metadatavalue_hk,
        item_metadatavalue_hashdiff,
        item_hk,
        item_bk,
        item_id,
        item_uuid,
        metadatavalue_hk,
        metadatavalue_bk,
        metadatafield_hk,
        metadatafield_bk,
        metadata_value_id,
        metadata_field_id,
        short_id,
        element,
        qualifier,
        metadatafield_fullname,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source_label AS source,
        load_datetime AS effective_from,
        load_datetime AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns=['item_bk', 'metadatavalue_bk'], alias='item_metadatavalue_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'item_bk',
                    'metadatavalue_bk',
                    'metadatafield_bk',
                    'metadatafield_fullname',
                    'text_value',
                    'text_lang',
                    'place',
                    'authority',
                    'confidence'
                ],
                alias='item_metadatavalue_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

SELECT * FROM final
