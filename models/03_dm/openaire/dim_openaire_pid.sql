{{ config(materialized = 'table') }}

-- ¿Qué identificadores persistentes y esquemas fueron registrados por OpenAIRE?
WITH pid AS (
    SELECT 
        value,
        scheme,
        pid_hk
    FROM {{ref('hub_openaire_pid')}} hub_pid
),

-- ¿Qué PIDs quedan disponibles para relacionarlos con researchproducts?
final AS (
    SELECT
        value,
        scheme,
        pid_hk
    FROM pid
)

SELECT *
FROM final
