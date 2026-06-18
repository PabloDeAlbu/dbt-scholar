{{ config(materialized='table') }}

-- Latest row from sat_dspacedb_collection__extract keyed by collection_hk + source_label + institution_ror.
WITH latest AS {{ latest_satellite(
    ref('sat_dspacedb_collection__extract'),
    'collection_hk, source_label, institution_ror',
    order_column='extract_datetime, load_datetime'
) }}

SELECT
    collection_hk,
    extract_cdk,
    extract_datetime,
    base_url,
    institution_ror,
    source_label,
    load_datetime,
    source
FROM latest
