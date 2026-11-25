-- ¿Qué tipos de instancia se observan para los researchproducts clasificados por FOS?
WITH researchproduct_subject_instance_type AS (
    SELECT
        researchproduct_id,
        subject_value,
        researchproduct_instance_type
    FROM {{ ref('openaire_researchproduct_subject_fos') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_instance_type') }}
        USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_researchproduct_instance_type') }}
        USING (researchproduct_instance_type_hk)
),

-- ¿Qué combinación de FOS y tipo de instancia queda disponible para cada researchproduct?
final AS (
    SELECT
        researchproduct_id,
        subject_value,
        researchproduct_instance_type
    FROM researchproduct_subject_instance_type
)

SELECT *
FROM final
