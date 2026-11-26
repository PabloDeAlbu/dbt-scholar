{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- ¿Qué eventos de extracción de researchproducts conserva el Data Vault de OpenAIRE?
WITH extraction AS (
    SELECT
        sat.extract_cdk,
        hub.researchproduct_id,
        sat._filter_param,
        sat._filter_value,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source,
        sat.researchproduct_hk
    FROM {{ ref('sat_openaire_researchproduct__extract') }} sat
    INNER JOIN {{ ref('hub_openaire_researchproduct') }} hub USING (researchproduct_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
),

-- ¿Qué atributos expone el hecho base de extracción de researchproducts?
final AS (
    SELECT
        extract_cdk,
        researchproduct_id,
        _filter_param,
        _filter_value,
        extract_datetime,
        load_datetime,
        source,
        researchproduct_hk
    FROM extraction
)

SELECT *
FROM final
