{{ config(materialized='table') }}

WITH latest_row AS (
    SELECT
        item_hk,
        item_id,
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
    FROM {{ ref('latest_sat_dspacedb5_item_metadatavalue') }}
),
final AS (
    SELECT *
    FROM latest_row
)

SELECT * FROM final
