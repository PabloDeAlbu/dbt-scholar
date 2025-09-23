WITH base AS (
    SELECT 
        researchproduct_id,
        instance_type as type
    FROM {{ ref('hub_openaire_researchproduct') }}
    LEFT JOIN {{ ref('brg_openaire_researchproduct_instances') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_instance_type') }} USING (instancetype_hk)
)

SELECT * FROM base
