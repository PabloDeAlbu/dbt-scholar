{{ config(materialized='table') }}

WITH RECURSIVE hierarchy AS (
    SELECT
        organization_id,
        organization_id AS ancestor_organization_id,
        0 AS distance,
        ARRAY[organization_id]::bigint[] AS organization_path_ids
    FROM {{ ref('dim_unlp_organization') }}

    UNION ALL

    SELECT
        hierarchy.organization_id,
        relationship.related_organization_id AS ancestor_organization_id,
        hierarchy.distance + 1 AS distance,
        ARRAY_APPEND(
            hierarchy.organization_path_ids,
            relationship.related_organization_id
        ) AS organization_path_ids
    FROM hierarchy
    INNER JOIN {{ ref('brg_unlp_organization_relationship') }} AS relationship
        ON relationship.organization_id = hierarchy.ancestor_organization_id
       AND relationship.relationship_type = 'pertenece_a'
    WHERE NOT relationship.related_organization_id = ANY(hierarchy.organization_path_ids)
),

deduplicated AS (
    SELECT DISTINCT ON (organization_id, ancestor_organization_id)
        organization_id,
        ancestor_organization_id,
        distance,
        organization_path_ids
    FROM hierarchy
    ORDER BY
        organization_id,
        ancestor_organization_id,
        distance,
        organization_path_ids
)

SELECT
    organization_id,
    ancestor_organization_id,
    distance,
    distance = 0 AS is_self,
    distance = 1 AS is_direct,
    organization_path_ids
FROM deduplicated
