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
        scope.source_label,
        scope.institution_ror,
        metadata.metadatafield_hk,
        metadata.metadatafield_fullname,
        metadata.short_id,
        metadata.element,
        metadata.qualifier,
        metadata.metadata_value_id,
        metadata.item_hk
    FROM unlp_item_scope AS scope
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS metadata
        USING (item_hk)
),
aggregated AS (
    SELECT
        metadatafield_hk,
        MIN(metadatafield_fullname) AS metadatafield_fullname,
        MIN(short_id) AS metadata_schema_short_id,
        MIN(element) AS element,
        MIN(qualifier) AS qualifier,
        COUNT(DISTINCT item_hk)::integer AS item_count,
        COUNT(DISTINCT metadata_value_id)::integer AS metadata_value_count,
        MIN(source_label) AS source_label,
        MIN(institution_ror) AS institution_ror
    FROM metadata_observation
    GROUP BY metadatafield_hk
)

SELECT * FROM aggregated
