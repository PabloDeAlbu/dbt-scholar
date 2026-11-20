WITH base AS (
    SELECT 
        hub.researchproduct_id,
        fct.type,
        dim_instance_type.instance_type
    FROM {{ ref('hub_openaire_researchproduct') }} hub
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} fct USING (researchproduct_hk)
    LEFT JOIN {{ ref('brg_openaire_researchproduct_instances') }} USING (researchproduct_hk)
    LEFT JOIN {{ ref('dim_openaire_instance_type') }} dim_instance_type USING (instancetype_hk)
)

SELECT * FROM base
