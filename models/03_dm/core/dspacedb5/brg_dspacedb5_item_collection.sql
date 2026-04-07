{{ config(materialized='table') }}

WITH relation_base AS (
    SELECT DISTINCT
        item_hk,
        item_id,
        collection_hk,
        collection_id,
        source_label,
        institution_ror
    FROM {{ ref('stg_dspacedb5_collection2item') }}
    WHERE item_id <> -1
      AND collection_id <> -1
),
counts AS (
    SELECT
        item_hk,
        COUNT(DISTINCT collection_hk) AS collections_count
    FROM relation_base
    GROUP BY item_hk
),
final AS (
    SELECT
        rb.item_hk,
        rb.item_id,
        rb.collection_hk,
        rb.collection_id,
        c.collections_count,
        rb.source_label,
        rb.institution_ror
    FROM relation_base rb
    LEFT JOIN counts c
        USING (item_hk)
)

SELECT * FROM final
