{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts y PIDs conserva OpenAIRE?
WITH relationship AS (
    SELECT 
        value,
        scheme,
        researchproduct_hk,
        pid_hk,
        researchproduct_pid_hk
    FROM {{ref('link_openaire_researchproduct_pid')}} 
    JOIN {{ref('hub_openaire_pid')}} USING (pid_hk)
),

-- ¿Qué PIDs y esquemas están asociados con cada researchproduct?
final AS (
    SELECT
        value,
        scheme,
        researchproduct_hk,
        pid_hk,
        researchproduct_pid_hk
    FROM relationship
)

SELECT *
FROM final
