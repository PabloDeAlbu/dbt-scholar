{{ config(materialized='view') }}

WITH final AS (
    SELECT
        item_id,
        metadata_value_id,
        text_value_raw AS sedici_contributor_compiler_raw,
        NULLIF({{ clean_text("REPLACE(text_value_raw, '|', ' ')") }}, '') AS sedici_contributor_compiler,
        text_lang_raw,
        text_lang,
        authority_raw,
        authority_uri,
        authority_host,
        authority_path,
        confidence,
        place
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'sedici.contributor.compiler'
)

SELECT *
FROM final
