{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre organizaciones y PIDs conserva OpenAIRE?
WITH relationship AS (
    SELECT
        organization_pid_hk,
        organization_hk,
        pid_hk
    FROM {{ ref('link_openaire_organization_pid') }}
),

-- ¿Qué esquema y valor corresponden a cada PID relacionado?
pid AS (
    SELECT
        pid_hk,
        scheme,
        value
    FROM {{ ref('hub_openaire_pid') }}
),

-- ¿Qué PIDs están asociados con cada organización de OpenAIRE?
final AS (
    SELECT
        relationship.organization_pid_hk,
        relationship.organization_hk,
        relationship.pid_hk,
        pid.scheme,
        pid.value
    FROM relationship
    INNER JOIN pid
        USING (pid_hk)
)

SELECT *
FROM final
