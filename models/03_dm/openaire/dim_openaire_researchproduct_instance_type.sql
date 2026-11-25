-- ¿Qué tipos de instancia de researchproduct fueron observados por OpenAIRE?
WITH researchproduct_instance_type AS (
    SELECT
        instancetype_hk AS researchproduct_instance_type_hk,
        type AS researchproduct_instance_type
    FROM {{ ref('hub_openaire_instancetype') }}
),

-- ¿Qué tipos de instancia quedan disponibles para clasificar las instancias de los researchproducts?
final AS (
    SELECT
        researchproduct_instance_type_hk,
        researchproduct_instance_type
    FROM researchproduct_instance_type
)

SELECT *
FROM final
