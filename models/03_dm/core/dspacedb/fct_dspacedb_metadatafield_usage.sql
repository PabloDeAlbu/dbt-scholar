{{ config(materialized='table') }}

WITH ghost_metadatafield AS (
    SELECT metadatafield_hk
    FROM {{ ref('stg_dspacedb_metadatafieldregistry') }}
    WHERE metadata_field_id = -1
      AND source_label = '!UNKNOWN'
      AND institution_ror = '!UNKNOWN'
),
metadata AS (
    SELECT
        metadatafield_hk,
        metadatafield_fullname,
        short_id,
        element,
        qualifier,
        metadata_value_id,
        metadata_field_id,
        item_hk,
        item_id,
        item_uuid,
        source_label,
        institution_ror
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_hk NOT IN (
        SELECT metadatafield_hk
        FROM ghost_metadatafield
    )
),
final AS (
    SELECT
        metadatafield_hk,
        metadatafield_fullname,
        CASE
            WHEN qualifier IS NOT NULL
                THEN short_id || '.' || element || '.' || qualifier
            ELSE short_id || '.' || element
        END AS metadatafield_name,
        short_id,
        metadata_field_id,
        element,
        qualifier,
        COUNT(*) AS metadata_value_count,
        COUNT(DISTINCT metadata_value_id) AS metadata_value_id_count,
        COUNT(DISTINCT item_hk) AS item_count,
        COUNT(DISTINCT item_id) AS item_id_count,
        COUNT(DISTINCT item_uuid) AS item_uuid_count,
        source_label,
        institution_ror
    FROM metadata
    GROUP BY 1,2,3,4,5,6,7,13,14
)

SELECT * FROM final
