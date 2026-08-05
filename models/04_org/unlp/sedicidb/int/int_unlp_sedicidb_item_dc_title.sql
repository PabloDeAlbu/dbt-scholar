{{ config(materialized='view') }}

WITH final AS (
    SELECT
        item_id,
        metadata_value_id,
        text_value_raw AS dc_title_raw,
        NULLIF({{ clean_text("REPLACE(text_value_raw, '|', ' ')") }}, '') AS dc_title,
        text_lang_raw,
        text_lang,
        place
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dc.title'
)

SELECT *
FROM final
