{{ config(materialized='table') }}

WITH base AS (
    SELECT item_hk
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
        b.item_hk,
        mv.text_value
    FROM base AS b
    INNER JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
        USING (item_hk)
    WHERE mv.metadatafield_fullname = 'dc.type'
      AND NULLIF(TRIM(mv.text_value), '') IS NOT NULL
)

SELECT DISTINCT dc_type.text_value
FROM dc_type
INNER JOIN id
    USING (item_hk)
