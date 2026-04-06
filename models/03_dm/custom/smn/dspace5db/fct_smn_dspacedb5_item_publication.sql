{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/028x29h02'
      AND source_label = 'SMN'
),

metadatafield_dates AS (
    SELECT
        metadatafield_hk,
        qualifier
    FROM {{ ref('dim_dspacedb5_metadatafield') }}
    WHERE short_id = 'dc'
      AND element = 'date'
      AND qualifier IN ('available', 'issued')
),

raw_dates AS (
    SELECT
        base.item_hk,
        base.item_id,
        md.qualifier,
        mv.text_value
    FROM base
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
        USING (item_hk)
    JOIN metadatafield_dates AS md
        USING (metadatafield_hk)
    WHERE mv.text_value IS NOT NULL
),

parsed_dates AS (
    SELECT
        item_hk,
        item_id,
        qualifier,
        text_value AS raw_value,
        {{ str_to_date('text_value') }} AS parsed_date
    FROM raw_dates
),

date_available AS (
    SELECT
        item_hk,
        MIN(parsed_date) AS dc_date_available,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dc_date_available_raw
    FROM parsed_dates
    WHERE qualifier = 'available'
      AND parsed_date <> DATE '9999-12-31'
    GROUP BY item_hk
),

date_issued AS (
    SELECT
        item_hk,
        MIN(parsed_date) AS dc_date_issued,
        STRING_AGG(DISTINCT raw_value, '|' ORDER BY raw_value) AS dc_date_issued_raw
    FROM parsed_dates
    WHERE qualifier = 'issued'
      AND parsed_date <> DATE '9999-12-31'
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
