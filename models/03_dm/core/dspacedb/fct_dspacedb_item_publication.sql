{{ config(materialized='table') }}

WITH item_hub AS (
    SELECT
        item_hk,
        SPLIT_PART(item_bk, '||', 3)::uuid AS item_uuid
    FROM {{ ref('hub_dspacedb_item') }}
),
latest_item_id AS (
    SELECT
        item_hk,
        item_id,
        ROW_NUMBER() OVER (
            PARTITION BY item_hk
            ORDER BY load_datetime DESC, extract_datetime DESC
        ) AS rn
    FROM {{ ref('stg_dspacedb_item') }}
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
    FROM {{ latest_satellite(ref('sat_dspacedb_item'), 'item_hk', order_column='load_datetime') }}
),
latest_item_extract AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY item_hk, source_label, institution_ror
            ORDER BY extract_datetime DESC, load_datetime DESC
        ) AS rn
    FROM {{ ref('sat_dspacedb_item__extract') }}
),
final AS (
    SELECT
        hub.item_hk,
        legacy.item_id,
        hub.item_uuid,
        sat.submitter_id,
        sat.in_archive,
        sat.withdrawn,
        sat.discoverable,
        sat.owning_collection,
        sat.last_modified,
        extract.source_label,
        extract.institution_ror,
        win.first_extract_datetime,
        win.last_extract_datetime,
        win.first_load_datetime,
        win.last_load_datetime
    FROM latest_item_extract AS extract
    INNER JOIN {{ ref('fct_dspacedb_item_extraction_window') }} AS win
        USING (item_hk, source_label, institution_ror)
    INNER JOIN latest_item_sat AS sat
        USING (item_hk)
    INNER JOIN item_hub AS hub
        USING (item_hk)
    LEFT JOIN latest_item_id AS legacy
        ON hub.item_hk = legacy.item_hk
       AND legacy.rn = 1
    WHERE extract.rn = 1
      AND sat.in_archive = true
      AND sat.withdrawn = false
)

SELECT * FROM final
