{{ config(materialized='table') }}

WITH organization AS (
    SELECT
        institution_node_id AS organization_id,
        nombre_institucion AS organization_name,
        abreviatura AS organization_acronym,
        pidu_id,
        esta_publicada AS is_active,
        nivel_arbol_unlp AS source_hierarchy_level,
        fecha_creacion AS source_created_at,
        fecha_actualizacion AS source_updated_at,
        load_datetime,
        'vocsedici'::text AS source
    FROM {{ ref('dim_vocsedici_institucion') }}
    WHERE pertenece_arbol_unlp IS TRUE
)

SELECT
    organization_id,
    organization_id AS vocsedici_institution_node_id,
    organization_name,
    organization_acronym,
    pidu_id,
    is_active,
    source_hierarchy_level,
    'http://voc.sedici.unlp.edu.ar/node/' || organization_id AS source_url,
    source_created_at,
    source_updated_at,
    load_datetime,
    source
FROM organization
