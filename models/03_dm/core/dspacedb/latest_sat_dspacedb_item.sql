{{ config(materialized='table') }}

-- Latest row from sat_dspacedb_item keyed by item_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb_item'), 'item_hk', order_column='load_datetime') }}

SELECT
    item_hk,
    submitter_id,
    in_archive,
    withdrawn,
    discoverable,
    last_modified,
    owning_collection,
    load_datetime,
    source
FROM latest
