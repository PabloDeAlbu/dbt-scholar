-- ¿Qué tipos generales y tipos de instancia se observan para cada researchproduct de OpenAIRE?
WITH researchproduct_instance_type AS (
    SELECT
        hub.researchproduct_id,
        fct.type AS researchproduct_type,
        dim_instance_type.researchproduct_instance_type
    FROM {{ ref('hub_openaire_researchproduct') }} AS hub
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} AS fct
        USING (researchproduct_hk)
    LEFT JOIN {{ ref('brg_openaire_researchproduct_instance_type') }}
        USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_researchproduct_instance_type') }} AS dim_instance_type
        USING (researchproduct_instance_type_hk)
),

-- ¿Qué clasificación general y de instancia queda disponible para cada researchproduct?
final AS (
    SELECT
        researchproduct_id,
        researchproduct_type,
        researchproduct_instance_type
    FROM researchproduct_instance_type
)

SELECT *
FROM final
