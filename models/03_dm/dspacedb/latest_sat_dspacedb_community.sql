{{ config(materialized='table') }}

-- Latest row from sat_dspacedb_community keyed by community_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb_community'), 'community_hk', order_column='load_datetime') }}

SELECT
    community_hk,
    community_id,
    community_uuid,
    admin,
    logo_bitstream_id,
    base_url,
    source_label,
    institution_ror,
    load_datetime,
    source
FROM latest
