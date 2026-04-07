{{ config(materialized='table') }}

WITH duplicate_keys AS (
    SELECT
        item_hk,
        COUNT(*) AS issue_count
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    GROUP BY item_hk
    HAVING COUNT(*) > 1
),
final AS (
    SELECT
        p.item_hk,
        p.item_id,
        p.source_label,
        p.institution_ror,
        p.owning_collection,
        p.owningcollection_hk,
        p.collections_count,
        p.first_extract_datetime,
        p.last_extract_datetime,
        p.first_load_datetime,
        p.last_load_datetime,
        d.issue_count
    FROM {{ ref('fct_dspacedb5_item_publication') }} p
    JOIN duplicate_keys d
        USING (item_hk)
)

SELECT * FROM final
