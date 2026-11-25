{{ config(materialized = 'table') }}

-- ¿Qué filtros de extracción quedaron registrados históricamente para cada researchproduct?
WITH filter_observation AS (
    SELECT
        dim_rp.researchproduct_id,
        sat.load_datetime,
        sat._filter_param,
        sat._filter_value,
        sat.source,
        dim_rp.researchproduct_hk
    FROM {{ ref('sat_openaire_researchproduct__filter_applied') }} sat
    INNER JOIN {{ ref('dim_openaire_researchproduct') }} dim_rp USING (researchproduct_hk)
),

-- ¿Qué observaciones históricas de filtros aplicados expone el satélite original de OpenAIRE?
final AS (
    SELECT
        researchproduct_id,
        load_datetime,
        _filter_param,
        _filter_value,
        source,
        researchproduct_hk
    FROM filter_observation
)

SELECT *
FROM final
