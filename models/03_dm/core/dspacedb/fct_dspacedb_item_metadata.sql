{{ config(materialized='table') }}

WITH latest_row AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_metadatavalue_hk
            ORDER BY load_datetime DESC
        ) AS rn
    FROM {{ ref('sat_dspacedb_item_metadatavalue') }}
),
final AS (
    SELECT
        item_hk,
        item_id,
        item_uuid,
        source_label,
        institution_ror,
        metadatavalue_hk,
        metadatafield_hk,
        metadata_value_id,
        metadata_field_id,
        metadatafield_fullname,
        short_id,
        element,
        qualifier,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        load_datetime
    FROM latest_row
    WHERE rn = 1
)

SELECT * FROM final
