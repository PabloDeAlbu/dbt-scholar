{{ config(materialized='table') }}

-- ¿Cuál es la ventana temporal observada para cada researchproduct y criterio de extracción?
WITH extraction_window AS (
    SELECT
        researchproduct_hk,
        _filter_param,
        _filter_value,
        MIN(extract_datetime) AS first_extract_datetime,
        MAX(extract_datetime) AS last_extract_datetime,
        MIN(load_datetime) AS first_load_datetime,
        MAX(load_datetime) AS last_load_datetime
    FROM {{ ref('fct_openaire_researchproduct_extraction') }}
    GROUP BY
        researchproduct_hk,
        _filter_param,
        _filter_value
),

-- ¿Qué atributos expone la ventana de extracciones de cada researchproduct?
final AS (
    SELECT
        researchproduct_hk,
        _filter_param,
        _filter_value,
        first_extract_datetime,
        last_extract_datetime,
        first_load_datetime,
        last_load_datetime
    FROM extraction_window
)

SELECT *
FROM final
