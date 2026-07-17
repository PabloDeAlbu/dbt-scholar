{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_hk'], 'unique': true},
        {'columns': ['institution_ror', 'source_label', 'base_url', 'item_id']},
        {'columns': ['institution_ror', 'source_label', 'base_url', 'owningcollection_hk']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH item_scope AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        owningcollection_hk
    FROM {{ ref('latest_sat_dspacedb5_item__scope') }}
),

latest_item_sat AS (
    SELECT
        item_hk,
        submitter_id,
        in_archive,
        withdrawn,
        discoverable,
        owning_collection,
        last_modified,
        load_datetime
    FROM {{ ref('latest_sat_dspacedb5_item') }}
),

extraction_window AS (
    SELECT
        source_label,
        institution_ror,
        base_url,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_dspacedb5_extraction') }}
    GROUP BY source_label, institution_ror, base_url
),
item_collection_counts AS (
    SELECT
        item_hk,
        MAX(collections_count) AS collections_count
    FROM {{ ref('brg_dspacedb5_item_collection') }}
    GROUP BY item_hk
),
owning_collection_bk AS (
    SELECT
        sat.item_hk,
        scope.source_label,
        scope.institution_ror,
        scope.base_url,
        scope.owningcollection_hk
    FROM latest_item_sat AS sat
    INNER JOIN item_scope AS scope
        USING (item_hk)
),
owning_collection_hk AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        base_url,
        owningcollection_hk
    FROM owning_collection_bk
),

final AS (
    SELECT
        scope.item_hk,
        scope.item_id,
        sat.submitter_id,
        sat.in_archive,
        sat.withdrawn,
        sat.discoverable,
        sat.owning_collection,
        owning.owningcollection_hk,
        COALESCE(icc.collections_count, 0) AS collections_count,
        sat.last_modified,
        scope.source_label,
        scope.institution_ror,
        scope.base_url,
        win.first_extract_datetime,
        win.last_extract_datetime,
        win.first_load_datetime,
        win.last_load_datetime
    FROM latest_item_sat AS sat
    INNER JOIN item_scope AS scope
        USING (item_hk)
    LEFT JOIN extraction_window AS win
        USING (source_label, institution_ror, base_url)
    LEFT JOIN item_collection_counts AS icc
        USING (item_hk)
    LEFT JOIN owning_collection_hk AS owning
        USING (item_hk, source_label, institution_ror, base_url)
)

SELECT * FROM final
