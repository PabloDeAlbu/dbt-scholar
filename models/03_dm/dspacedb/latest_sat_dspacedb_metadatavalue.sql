{{ config(materialized='table') }}

-- Latest row from sat_dspacedb_metadatavalue keyed by metadatavalue_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb_metadatavalue'), 'metadatavalue_hk', order_column='extract_datetime, load_datetime') }}

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
    base_url,
    source_label,
    institution_ror,
    extract_datetime,
    load_datetime,
    source
FROM latest
