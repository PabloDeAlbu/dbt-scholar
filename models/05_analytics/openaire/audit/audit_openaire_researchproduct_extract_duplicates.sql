{{ config(materialized='view') }}

-- ¿Qué observaciones crudas componen las extracciones de researchproducts de OpenAIRE?
WITH researchproduct_observation AS (
    SELECT
        id::text AS researchproduct_id,
        _filter_param::text AS _filter_param,
        _filter_value::text AS _filter_value,
        _load_datetime::timestamp AS load_datetime,
        _extract_datetime::timestamp AS extract_datetime
    FROM {{ source('openaire', 'researchproduct') }}
),

-- ¿Qué observaciones se repiten para el mismo researchproduct, criterio y momento de extracción?
duplicate_observation AS (
    SELECT
        researchproduct_id,
        extract_datetime,
        _filter_param,
        _filter_value,
        load_datetime,
        COUNT(*) AS row_count
    FROM researchproduct_observation
    GROUP BY
        researchproduct_id,
        extract_datetime,
        _filter_param,
        _filter_value,
        load_datetime
    HAVING COUNT(*) > 1
),

-- ¿Qué atributos permiten auditar las observaciones duplicadas?
final AS (
    SELECT
        researchproduct_id,
        extract_datetime,
        _filter_param,
        _filter_value,
        load_datetime,
        row_count
    FROM duplicate_observation
)

SELECT *
FROM final
