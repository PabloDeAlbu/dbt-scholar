{{ config(materialized='table') }}

WITH smn_items AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror
    FROM {{ ref('fct_smn_dspacedb5_item_publication') }}
),

smn_metadata AS (
    SELECT
        md.item_hk,
        md.item_id,
        md.metadatafield_hk,
        md.metadatafield_fullname,
        md.short_id,
        md.element,
        md.qualifier,
        md.metadata_value_id
    FROM {{ ref('fct_dspacedb5_item_metadata') }} AS md
    INNER JOIN smn_items AS smn
        USING (item_hk)
),

final AS (
    SELECT
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        element,
        qualifier,
        COUNT(*) AS metadata_value_count,
        COUNT(DISTINCT item_hk) AS item_count,
        COUNT(DISTINCT item_id) AS item_id_count
    FROM smn_metadata
    GROUP BY 1,2,3,4,5
)

SELECT
    final.*,
    smn.source_label,
    smn.institution_ror
FROM final
CROSS JOIN (
    SELECT DISTINCT source_label, institution_ror
    FROM smn_items
) AS smn
