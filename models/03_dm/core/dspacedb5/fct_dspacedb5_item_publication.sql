{{ config(materialized='table') }}

WITH item_hub AS (
    SELECT
        item_hk,
        SPLIT_PART(item_bk, '||', 3)::bigint AS item_id
    FROM {{ ref('hub_dspacedb5_item') }}
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

latest_item_extract AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('latest_sat_dspacedb5_item__extract') }}
),
extraction_window AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_dspacedb5_item_extraction') }}
    GROUP BY item_hk, source_label, institution_ror
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
        item_hk,
        source_label,
        institution_ror,
        institution_ror || '||' || source_label || '||' || owning_collection::text AS owningcollection_bk
    FROM latest_item_extract AS extract
    INNER JOIN latest_item_sat AS sat
        USING (item_hk)
),
owning_collection_hk AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        {{ automate_dv.hash(columns='owningcollection_bk', alias='owningcollection_hk') }}
    FROM owning_collection_bk
),

final AS (
    SELECT
        hub.item_hk,
        hub.item_id,
        sat.submitter_id,
        sat.in_archive,
        sat.withdrawn,
        sat.discoverable,
        sat.owning_collection,
        owning.owningcollection_hk,
        COALESCE(icc.collections_count, 0) AS collections_count,
        sat.last_modified,
        extract.source_label,
        extract.institution_ror,
        win.first_extract_datetime,
        win.last_extract_datetime,
        win.first_load_datetime,
        win.last_load_datetime
    FROM latest_item_extract AS extract
    INNER JOIN extraction_window AS win
        USING (item_hk, source_label, institution_ror)
    INNER JOIN latest_item_sat AS sat
        USING (item_hk)
    INNER JOIN item_hub AS hub
        USING (item_hk)
    LEFT JOIN item_collection_counts AS icc
        USING (item_hk)
    LEFT JOIN owning_collection_hk AS owning
        USING (item_hk, source_label, institution_ror)
    WHERE sat.in_archive = true
      AND sat.withdrawn = false
)

SELECT * FROM final
