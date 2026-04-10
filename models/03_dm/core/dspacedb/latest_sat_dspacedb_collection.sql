{{ config(materialized='table') }}

-- Latest row from sat_dspacedb_collection keyed by collection_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb_collection'), 'collection_hk', order_column='load_datetime') }}

SELECT
    collection_hk,
    collection_id,
    collection_uuid,
    submitter,
    template_item_id,
    logo_bitstream_id,
    admin,
    base_url,
    source_label,
    institution_ror,
    load_datetime,
    source
FROM latest
