WITH base AS (
    SELECT 
        researchproduct_id,
        subject_value,
        type
    FROM {{ ref('viz_openaire_researchproduct_subject_fos') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_instances') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_instance_type') }} USING (instancetype_hk)
)

SELECT * FROM base
