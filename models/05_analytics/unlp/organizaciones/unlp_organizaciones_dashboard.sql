{{ config(materialized='table') }}

SELECT
    organization.organization_id AS id_organizacion,
    organization.organization_name AS organizacion,
    organization.organization_acronym AS abreviatura,
    organization.pidu_id AS id_pidu,
    organization.is_active AS esta_publicada_en_voc,
    organization.source_hierarchy_level AS nivel_en_voc,
    organization.source_url AS url_en_voc,
    ancestor.organization_id AS id_organizacion_antecesora,
    ancestor.organization_name AS organizacion_antecesora,
    hierarchy.distance AS distancia,
    hierarchy.is_self AS es_la_misma_organizacion,
    hierarchy.is_direct AS es_relacion_directa,
    hierarchy.organization_path_ids AS ruta_ids,
    organization.source AS fuente
FROM {{ ref('brg_unlp_organization_hierarchy') }} AS hierarchy
INNER JOIN {{ ref('dim_unlp_organization') }} AS organization
    ON organization.organization_id = hierarchy.organization_id
INNER JOIN {{ ref('dim_unlp_organization') }} AS ancestor
    ON ancestor.organization_id = hierarchy.ancestor_organization_id
