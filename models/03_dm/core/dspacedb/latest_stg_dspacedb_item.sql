{{ config(materialized='table') }}

-- Latest row from stg_dspacedb_item keyed by item_hk.
WITH latest AS {{ latest_satellite(ref('stg_dspacedb_item'), 'item_hk', order_column='load_datetime, extract_datetime') }}

SELECT
    item_hk,
    item_hashdiff,
    item_bk,
    item_id,
    item_uuid,
    in_archive,
    withdrawn,
    last_modified,
    discoverable,
    submitter_id,
    owning_collection,
    source_label,
    institution_ror,
    extract_datetime,
    load_datetime,
    source,
    effective_from,
    start_date,
    end_date
FROM latest
