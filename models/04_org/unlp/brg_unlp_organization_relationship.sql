{{ config(materialized='table') }}

WITH organization AS (
    SELECT organization_id
    FROM {{ ref('dim_unlp_organization') }}
),

relationship AS (
    SELECT
        institution.institution_node_id AS organization_id,
        institution.parent_institution_node_id AS related_organization_id,
        'pertenece_a'::text AS relationship_type,
        'vocsedici'::text AS source
    FROM {{ ref('dim_vocsedici_institucion') }} AS institution
    INNER JOIN organization AS child
        ON child.organization_id = institution.institution_node_id
    INNER JOIN organization AS parent
        ON parent.organization_id = institution.parent_institution_node_id
    WHERE institution.parent_institution_node_id IS NOT NULL
)

SELECT * FROM relationship
