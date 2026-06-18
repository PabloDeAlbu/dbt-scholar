{{ config(materialized='table') }}

-- Latest row from sat_dspacedb5_item__extract keyed by item_hk + source_label + institution_ror.
WITH latest AS {{ latest_satellite(
    ref('sat_dspacedb5_item__extract'),
    'item_hk, source_label, institution_ror',
    order_column='extract_datetime, load_datetime'
) }}

SELECT
    item_hk,
    extract_cdk,
    extract_datetime,
    institution_ror,
    source_label,
    load_datetime,
    source
FROM latest
