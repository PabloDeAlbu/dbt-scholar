{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        dspace_object_id,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || metadata_value_id::text AS metadatavalue_bk,
        _institution_ror || '||' || _source_label || '||' || metadata_field_id::text AS metadatafield_bk,
        _institution_ror || '||' || _source_label || '||' || dspace_object_id::text AS dspaceobject_bk
    FROM {{ ref('ldg_dspacedb_metadatavalue') }}
),
final AS (
    SELECT
        metadatavalue_hk,
        metadatafield_hk,
        dspaceobject_hk,
        metadatavalue_bk,
        metadatafield_bk,
        dspaceobject_bk,
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        dspace_object_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source_label AS source,
        extract_datetime AS effective_from,
        extract_datetime AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='metadatavalue_bk', alias='metadatavalue_hk') }},
            {{ automate_dv.hash(columns='metadatafield_bk', alias='metadatafield_hk') }},
            {{ automate_dv.hash(columns='dspaceobject_bk', alias='dspaceobject_hk') }}
    ) s1
)

SELECT * FROM final
