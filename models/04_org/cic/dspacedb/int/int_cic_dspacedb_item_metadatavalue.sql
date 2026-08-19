{{ config(materialized='view') }}

SELECT
    item.item_hk,
    item.item_uuid,
    metadata.metadatavalue_hk,
    metadata.metadata_value_id,
    metadata.metadata_field_id,
    metadata.metadatafield_fullname,
    metadata.text_value AS value_raw,
    CASE
        WHEN metadata.text_value = '!UNKNOWN' THEN NULL
        ELSE NULLIF({{ clean_text("REPLACE(metadata.text_value, '|', ' ')") }}, '')
    END AS value,
    metadata.text_lang AS text_lang_raw,
    NULLIF(LOWER(BTRIM(metadata.text_lang)), '') AS text_lang,
    metadata.authority AS authority_raw,
    NULLIF(BTRIM(metadata.authority), '') AS authority,
    metadata.confidence,
    metadata.place
FROM {{ ref('int_cic_dspacedb_item') }} AS item
INNER JOIN {{ ref('fct_dspacedb_item_metadata') }} AS metadata
    ON metadata.item_hk = item.item_hk
   AND metadata.source_label = item.source_label
   AND metadata.institution_ror = item.institution_ror
