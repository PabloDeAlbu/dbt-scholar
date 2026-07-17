{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_hk'], 'unique': true},
        {'columns': ['institution_ror', 'source_label', 'base_url', 'item_id']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- Latest scope row from sat_dspacedb5_item__scope keyed by item_hk.
WITH latest AS {{ latest_satellite(ref('sat_dspacedb5_item__scope'), 'item_hk', order_column='load_datetime, effective_from') }}

SELECT
    item_hk,
    item_id,
    source_label,
    institution_ror,
    base_url,
    owningcollection_hk,
    effective_from,
    load_datetime,
    source
FROM latest
