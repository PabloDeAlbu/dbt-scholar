{{ config(materialized='table') }}

WITH base AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
      AND discoverable = TRUE
      AND in_archive = TRUE
      AND withdrawn = FALSE
),
id AS (
    SELECT DISTINCT
        b.item_hk
    FROM base AS b
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
        USING (item_hk)
    WHERE mv.metadatafield_fullname = 'dc.identifier.uri'
      AND mv.text_value ~ '^https?://sedici[.]unlp[.]edu[.]ar/handle/10915'
),
dc_type AS (
    SELECT DISTINCT
        b.item_hk
    FROM base AS b
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
        USING (item_hk)
    WHERE mv.metadatafield_fullname = 'dc.type'
      AND NULLIF(TRIM(mv.text_value), '') IS NOT NULL
),
unlp_item_scope AS (
    SELECT
        b.item_hk,
        b.item_id,
        b.source_label,
        b.institution_ror
    FROM base AS b
    INNER JOIN id USING (item_hk)
    INNER JOIN dc_type USING (item_hk)
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
        metadata.short_id AS metadata_schema_short_id,
        metadata.element,
        metadata.qualifier,
        metadata.text_value,
        metadata.text_lang,
        metadata.place,
        metadata.authority,
        metadata.confidence
    FROM unlp_item_scope AS scope
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS metadata
        USING (item_hk)
)

SELECT * FROM metadata_observation
