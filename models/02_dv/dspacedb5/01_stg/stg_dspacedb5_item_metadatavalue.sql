{{ config(materialized='view') }}

WITH item AS (
    SELECT
        item_bk,
        item_id,
        _source_label AS source_label,
        _institution_ror AS institution_ror
    FROM {{ ref('stg_dspacedb5_item') }}
),
metadatafield AS (
    SELECT
        mf.metadatafield_bk,
        ms.short_id,
        mf.element,
        mf.qualifier,
        CASE
            WHEN mf.qualifier IS NOT NULL
                THEN ms.short_id || '.' || mf.element || '.' || mf.qualifier
            ELSE ms.short_id || '.' || mf.element
        END AS metadatafield_fullname
    FROM {{ ref('stg_dspacedb5_metadatafieldregistry') }} AS mf
    JOIN {{ ref('stg_dspacedb5_metadataschemaregistry') }} AS ms
        USING (metadataschema_hk)
),
raw_source AS (
    SELECT
        it.item_bk,
        it.item_id,
        mv.metadatavalue_bk,
        mv.metadatafield_bk,
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
        mv._extract_datetime AS extract_datetime,
        mv._load_datetime AS load_datetime,
        it.source_label,
        it.institution_ror
    FROM {{ ref('stg_dspacedb5_metadatavalue') }} AS mv
    JOIN item AS it
        ON mv.resource_bk = it.item_bk
    JOIN metadatafield AS mf
        ON mv.metadatafield_bk = mf.metadatafield_bk
    WHERE mv.resource_type_id = 2
),
final AS (
    SELECT
        item_hk,
        metadatafield_hk,
        metadatavalue_hk,
        item_metadatavalue_hk,
        item_metadatavalue_extract_hk,
        item_metadatavalue_hashdiff,
        item_bk,
        item_id,
        metadatavalue_bk,
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
        extract_datetime,
        load_datetime,
        source_label,
        institution_ror,
        source_label AS source,
        extract_datetime AS effective_from,
        extract_datetime AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='item_bk', alias='item_hk') }},
            {{ automate_dv.hash(columns='metadatafield_bk', alias='metadatafield_hk') }},
            {{ automate_dv.hash(columns='metadatavalue_bk', alias='metadatavalue_hk') }},
            {{ automate_dv.hash(columns=['item_bk', 'metadatavalue_bk'], alias='item_metadatavalue_hk') }},
            {{ automate_dv.hash(columns=['item_bk', 'metadatavalue_bk', 'extract_datetime'], alias='item_metadatavalue_extract_hk') }},
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
