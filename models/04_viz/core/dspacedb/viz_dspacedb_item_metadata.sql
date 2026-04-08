{{ config(materialized='table') }}

WITH metadata AS (
    SELECT
        item_id,
        item_uuid,
        metadatafield_fullname,
        text_value,
        authority,
        place
    FROM {{ ref('fct_dspacedb_item_metadata') }}
),

final AS (
    SELECT
        metadata.item_id,
        metadata.item_uuid,
        metadata.metadatafield_fullname,
        metadata.text_value,
        metadata.authority,
        metadata.place
    FROM metadata
)

SELECT * FROM final
