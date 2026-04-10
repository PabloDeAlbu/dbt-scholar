{{ config(materialized='table') }}

WITH item_hub AS (
    SELECT
        item_hk,
        SPLIT_PART(item_bk, '||', 3)::uuid AS item_uuid
    FROM {{ ref('hub_dspacedb_item') }}
),
latest_item_sat AS (
    SELECT
        item_hk,
        item_id,
        submitter_id,
        in_archive,
        withdrawn,
        discoverable,
        owning_collection,
        last_modified,
        load_datetime
    FROM {{ ref('latest_sat_dspacedb_item') }}
),
latest_item_extract AS (
    SELECT
        item_hk,
        base_url,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('latest_sat_dspacedb_item__extract') }}
),
extraction_window AS (
    SELECT
        item_hk,
        base_url,
        source_label,
        institution_ror,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_dspacedb_item_extraction') }}
    GROUP BY item_hk, base_url, source_label, institution_ror
),
final AS (
    SELECT
        hub.item_hk,
        sat.item_id,
        hub.item_uuid,
        sat.submitter_id,
        sat.in_archive,
        sat.withdrawn,
        sat.discoverable,
        sat.owning_collection,
        sat.last_modified,
        extract.base_url,
        extract.source_label,
        extract.institution_ror,
        win.first_extract_datetime,
        win.last_extract_datetime,
        win.first_load_datetime,
        win.last_load_datetime
    FROM latest_item_extract AS extract
    INNER JOIN extraction_window AS win
        USING (item_hk, base_url, source_label, institution_ror)
    INNER JOIN latest_item_sat AS sat
        USING (item_hk)
    INNER JOIN item_hub AS hub
        USING (item_hk)
    WHERE sat.in_archive = true
      AND sat.withdrawn = false
)

SELECT * FROM final
