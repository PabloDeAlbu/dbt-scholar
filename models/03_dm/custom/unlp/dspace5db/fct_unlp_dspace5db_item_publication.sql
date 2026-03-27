{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
),

metadatafield_dates AS (
    SELECT
        mf.metadata_field_id,
        ms.short_id,
        mf.element,
        mf.qualifier
    FROM {{ ref('ldg_dspacedb5_metadatafieldregistry') }} AS mf
    JOIN {{ ref('ldg_dspacedb5_metadataschemaregistry') }} AS ms
        ON mf.metadata_schema_id = ms.metadata_schema_id
    WHERE ms.short_id = 'dc'
      AND mf.element = 'date'
      AND mf.qualifier IN ('available', 'issued')
),

raw_dates AS (
    SELECT
        base.item_hk,
        base.item_id,
        md.qualifier,
        mv.text_value
    FROM base
    JOIN {{ ref('ldg_dspacedb5_metadatavalue') }} AS mv
        ON base.item_id = mv.resource_id
    JOIN metadatafield_dates AS md
        ON mv.metadata_field_id = md.metadata_field_id
    WHERE mv.resource_type_id = 2
      AND mv.text_value IS NOT NULL
),

parsed_dates AS (
    SELECT
        item_hk,
        item_id,
        qualifier,
        text_value AS raw_value,
        CASE
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
                THEN TO_DATE(REPLACE(LEFT(text_value, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
                THEN TO_DATE(REPLACE(text_value, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}$'
                THEN TO_DATE(text_value || '-01-01', 'YYYY-MM-DD')
        END AS parsed_date
    FROM raw_dates
),

date_available AS (
    SELECT
        item_hk,
        MIN(parsed_date) AS dc_date_available,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dc_date_available_raw
    FROM parsed_dates
    WHERE qualifier = 'available'
      AND parsed_date IS NOT NULL
    GROUP BY item_hk
),

date_issued AS (
    SELECT
        item_hk,
        MIN(parsed_date) AS dc_date_issued,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dc_date_issued_raw
    FROM parsed_dates
    WHERE qualifier = 'issued'
      AND parsed_date IS NOT NULL
    GROUP BY item_hk
),

final AS (
    SELECT
        base.*,
        da.dc_date_available,
        da.dc_date_available_raw,
        di.dc_date_issued,
        di.dc_date_issued_raw
    FROM base
    LEFT JOIN date_available AS da USING (item_hk)
    LEFT JOIN date_issued AS di USING (item_hk)
)

SELECT * FROM final
