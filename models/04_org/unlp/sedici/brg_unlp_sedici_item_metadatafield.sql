{{ config(materialized='table') }}

WITH metadata_base AS (
    SELECT
        item_hk,
        metadata_value_id,
        metadatafield_hk,
        metadatafield_fullname,
        short_id AS metadata_schema_short_id,
        element,
        qualifier,
        text_value,
        text_lang,
        place,
        authority,
        confidence
    FROM {{ ref('fct_dspacedb5_item_metadata') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
),

item_scope AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror
    FROM {{ ref('fct_unlp_sedici_item_publication') }}
),

metadata_observation AS (
    SELECT
        scope.item_hk,
        scope.item_id,
        scope.source_label,
        scope.institution_ror,
        metadata.metadata_value_id,
        metadata.metadatafield_hk,
        metadata.metadatafield_fullname,
        metadata.metadata_schema_short_id,
        metadata.element,
        metadata.qualifier,
        metadata.text_value,
        metadata.text_lang,
        metadata.place,
        metadata.authority,
        metadata.confidence
    FROM item_scope AS scope
    INNER JOIN metadata_base AS metadata
        USING (item_hk)
)

SELECT *
FROM metadata_observation
