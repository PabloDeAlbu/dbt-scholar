{{ config(materialized='table') }}

WITH base AS (
    SELECT
        researchproduct_hk,
        _filter_param,
        _filter_value,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_openaire_researchproduct_extraction') }}
    GROUP BY researchproduct_hk, _filter_param, _filter_value
)

SELECT * FROM base
