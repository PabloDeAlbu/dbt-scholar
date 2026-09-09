{{ config(materialized='table') }}

-- Unidades organizativas UNLP identificadas mediante dominios institucionales curados.

WITH organizational_unit AS (
    SELECT
        dependency_domain AS organizational_unit_domain,
        dependency_domain_label AS organizational_unit_domain_label,
        dependency_name AS organizational_unit_name,
        parent_unit_name AS parent_organizational_unit_name
    FROM {{ ref('seed_unlp_domain_unit_map') }}
),

final AS (
    SELECT
        organizational_unit_domain,
        organizational_unit_domain_label,
        organizational_unit_name,
        parent_organizational_unit_name,
        'https://ror.org/01tjs6929'::text AS organization_ror
    FROM organizational_unit
)

SELECT *
FROM final
ORDER BY organizational_unit_domain
