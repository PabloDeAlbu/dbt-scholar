{{ config(materialized = 'view') }}

WITH base AS (
    SELECT 
        researchproduct_id,
        datasource_id,
        datasource_name
    FROM {{ ref('hub_openaire_researchproduct') }}
    INNER JOIN {{ ref('brg_openaire_researchproduct_datasource') }} brg USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_datasource') }} USING (datasource_hk)
)

SELECT * FROM base