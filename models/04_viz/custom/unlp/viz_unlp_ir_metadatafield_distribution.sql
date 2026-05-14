{{ config(materialized='table') }}

WITH base AS (
    SELECT
        bridge.item_hk,
        bridge.item_id,
        publication.date_accessioned,
        publication.date_issued,
        COALESCE(NULLIF(TRIM(publication.type), ''), '(sin dc.type)') AS dc_type,
        COALESCE(NULLIF(TRIM(publication.subtype), ''), '(sin sedici.subtype)') AS sedici_subtype,
        bridge.metadatafield_hk,
        bridge.metadatafield_fullname,
        bridge.metadata_schema_short_id,
        bridge.element,
        bridge.qualifier,
        NULLIF(TRIM(bridge.text_value), '') AS text_value,
        NULLIF(TRIM(bridge.text_lang), '') AS text_lang,
        NULLIF(TRIM(bridge.authority), '') AS authority,
        bridge.confidence
    FROM {{ ref('brg_unlp_ir_item_metadatafield') }} AS bridge
    INNER JOIN {{ ref('fct_unlp_ir_item_publication') }} AS publication
        USING (item_hk)
),

final AS (
    SELECT
        metadatafield_hk,
        metadatafield_fullname,
        metadata_schema_short_id,
        element,
        qualifier,
        COUNT(*)::bigint AS metadata_value_count,
        COUNT(DISTINCT item_hk)::bigint AS item_count,
        COUNT(DISTINCT item_id)::bigint AS item_id_count,
        COUNT(DISTINCT text_value)::bigint AS distinct_text_value_count,
        COUNT(*) FILTER (WHERE text_value IS NULL)::bigint AS blank_text_value_count,
        COUNT(*) FILTER (WHERE authority IS NOT NULL)::bigint AS authority_controlled_value_count,
        COUNT(DISTINCT dc_type)::bigint AS distinct_dc_type_count,
        COUNT(DISTINCT sedici_subtype)::bigint AS distinct_sedici_subtype_count,
        MIN(date_accessioned) AS min_date_accessioned,
        MAX(date_accessioned) AS max_date_accessioned,
        MIN(date_issued) AS min_date_issued,
        MAX(date_issued) AS max_date_issued
    FROM base
    GROUP BY
        metadatafield_hk,
        metadatafield_fullname,
        metadata_schema_short_id,
        element,
        qualifier
)

SELECT * FROM final
