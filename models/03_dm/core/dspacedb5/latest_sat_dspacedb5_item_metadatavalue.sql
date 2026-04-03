{{ config(materialized='table') }}

-- Latest row from sat_dspacedb5_item_metadatavalue keyed by item_metadatavalue_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb5_item_metadatavalue'), 'item_metadatavalue_hk', order_column='load_datetime') }}

SELECT
    item_metadatavalue_hk,
    item_hk,
    item_id,
    metadatavalue_hk,
    metadatafield_hk,
    metadata_value_id,
    metadata_field_id,
    metadatafield_fullname,
    short_id,
    element,
    qualifier,
    text_value,
    text_lang,
    place,
    authority,
    confidence,
    source_label,
    institution_ror,
    load_datetime,
    source
FROM latest
