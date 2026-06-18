{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dim_rp.researchproduct_id,
        sat.load_datetime,
        sat._filter_param,
        sat._filter_value,
        sat.source,
        dim_rp.researchproduct_hk
    FROM {{ ref('sat_openaire_researchproduct__filter_applied') }} sat
    INNER JOIN {{ ref('dim_openaire_researchproduct') }} dim_rp USING (researchproduct_hk)
)

SELECT * FROM base
