{{ config(materialized='table') }}

-- ¿Qué autoridades de autor tienen correspondencia con una persona de VOC SEDICI?
WITH author AS (
    SELECT
        authority,
        voc_person_node_id
    FROM {{ ref('dim_unlp_author') }}
    WHERE has_voc_match IS TRUE
),

-- ¿Qué afiliaciones publicadas vinculan personas e instituciones en VOC SEDICI?
affiliation AS (
    SELECT
        paragraph_id AS affiliation_id,
        person_node_id AS voc_person_node_id,
        institution_node_id,
        nombre_institucion AS institution_name,
        fecha_inicio AS valid_from,
        fecha_fin AS valid_to
    FROM {{ ref('fct_vocsedici_persona_afiliacion') }}
    WHERE afiliacion_esta_publicada IS TRUE
      AND person_esta_publicada IS TRUE
      AND institution_esta_publicada IS TRUE
),

-- ¿Qué contexto institucional aporta VOC SEDICI para cada afiliación?
institution AS (
    SELECT
        institution_node_id,
        parent_institution_node_id,
        nombre_institucion_padre AS parent_institution_name,
        unidad_principal_unlp_node_id AS main_unlp_unit_node_id,
        unidad_principal_unlp_nombre AS main_unlp_unit_name
    FROM {{ ref('dim_vocsedici_institucion') }}
),

-- ¿Qué instituciones observadas pertenecen al árbol organizacional de la UNLP?
unlp_organization AS (
    SELECT
        organization_id
    FROM {{ ref('dim_unlp_organization') }}
),

-- ¿Qué afiliaciones institucionales pueden atribuirse a cada autoridad de autor?
final AS (
    SELECT
        author.authority,
        author.voc_person_node_id,
        affiliation.affiliation_id,
        affiliation.institution_node_id,
        affiliation.institution_name,
        unlp_organization.organization_id AS unlp_organization_id,
        institution.parent_institution_node_id,
        institution.parent_institution_name,
        institution.main_unlp_unit_node_id,
        institution.main_unlp_unit_name,
        unlp_organization.organization_id IS NOT NULL AS is_unlp_affiliation,
        affiliation.valid_from,
        affiliation.valid_to,
        'vocsedici'::text AS affiliation_source
    FROM author
    INNER JOIN affiliation
        USING (voc_person_node_id)
    INNER JOIN institution
        USING (institution_node_id)
    LEFT JOIN unlp_organization
        ON unlp_organization.organization_id = affiliation.institution_node_id
)

SELECT *
FROM final
