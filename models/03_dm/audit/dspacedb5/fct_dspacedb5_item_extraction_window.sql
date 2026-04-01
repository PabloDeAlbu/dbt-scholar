{{ config(materialized='table') }}

WITH base AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('sat_dspacedb5_item__extract') }}
    GROUP BY item_hk, source_label, institution_ror
)

SELECT * FROM base
