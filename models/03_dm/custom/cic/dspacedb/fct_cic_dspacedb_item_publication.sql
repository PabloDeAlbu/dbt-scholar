{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb_item_publication') }}
    WHERE institution_ror = 'https://ror.org/02s7sax82'
),

owning_collection AS (
    SELECT
        base.item_uuid,
        MIN(col.collection_title) AS owning_collection_title
    FROM base
    LEFT JOIN {{ ref('dim_dspacedb_collection') }} AS col
        ON base.owning_collection = col.collection_uuid
       AND base.base_url = col.base_url
       AND base.institution_ror = col.institution_ror
    GROUP BY base.item_uuid
),

metadatafield_title AS (
    SELECT
        metadatafield_hk
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE short_id = 'dc'
      AND element = 'title'
      AND COALESCE(qualifier, '') IN ('', '!UNKNOWN')
    GROUP BY 1
),

metadatafield_dates AS (
    SELECT
        metadatafield_hk,
        short_id,
        element,
        qualifier
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE (short_id = 'dc' AND element = 'date' AND qualifier = 'available')
       OR (
           short_id = 'dcterms'
           AND element = 'issued'
           AND COALESCE(qualifier, '') IN ('', '!UNKNOWN')
       )
    GROUP BY 1, 2, 3, 4
),

title_agg AS (
    SELECT
        base.item_uuid,
        MIN(mv.text_value) AS dc_title,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS dc_title_raw
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_uuid = mv.item_uuid
    JOIN metadatafield_title AS mft
        ON mv.metadatafield_hk = mft.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
    GROUP BY base.item_uuid
),

raw_dates AS (
    SELECT
        base.item_uuid,
        md.short_id,
        md.element,
        md.qualifier,
        mv.text_value
    FROM base
    JOIN {{ ref('fct_dspacedb_item_metadata') }} AS mv
        ON base.item_hk = mv.item_hk
    JOIN metadatafield_dates AS md
        ON mv.metadatafield_hk = md.metadatafield_hk
    WHERE mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
),

parsed_dates AS (
    SELECT
        item_uuid,
        short_id,
        element,
        qualifier,
        text_value AS raw_value,
        {{ str_to_date('text_value') }} AS parsed_date
    FROM raw_dates
),

date_available AS (
    SELECT
        item_uuid,
        MIN(parsed_date) AS dc_date_available,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dc_date_available_raw
    FROM parsed_dates
    WHERE short_id = 'dc'
      AND element = 'date'
      AND qualifier = 'available'
      AND parsed_date <> DATE '9999-12-31'
    GROUP BY item_uuid
),

date_issued AS (
    SELECT
        item_uuid,
        MIN(parsed_date) AS dcterms_issued,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dcterms_issued_raw
    FROM parsed_dates
    WHERE short_id = 'dcterms'
      AND element = 'issued'
      AND COALESCE(qualifier, '') IN ('', '!UNKNOWN')
      AND parsed_date <> DATE '9999-12-31'
    GROUP BY item_uuid
),

final AS (
    SELECT
        base.*,
        owning.owning_collection_title,
        available.dc_date_available,
        available.dc_date_available_raw,
        issued.dcterms_issued,
        issued.dcterms_issued_raw,
        title.dc_title,
        title.dc_title_raw
    FROM base
    LEFT JOIN owning_collection AS owning
        USING (item_uuid)
    LEFT JOIN date_available AS available
        USING (item_uuid)
    LEFT JOIN date_issued AS issued
        USING (item_uuid)
    LEFT JOIN title_agg AS title
        USING (item_uuid)
)

SELECT * FROM final
