{{ config(materialized='table') }}

WITH persona AS (
    SELECT *
    FROM {{ ref('dim_vocsedici_persona') }}
),

afiliacion AS (
    SELECT *
    FROM {{ ref('fct_vocsedici_persona_afiliacion') }}
),

institucion AS (
    SELECT *
    FROM {{ ref('dim_vocsedici_institucion') }}
)

SELECT
    persona.person_node_id AS id_persona_voc,
    persona.nombre_completo AS persona,
    persona.esta_publicada AS persona_publicada,
    persona.persona_url AS url_persona_voc,
    persona.fecha_creacion AS fecha_registro_en_voc,
    persona.fecha_actualizacion AS fecha_actualizacion_en_voc,
    persona.orcid,
    persona.orcid IS NOT NULL AS tiene_orcid,
    persona.email AS correo_electronico,
    persona.email IS NOT NULL AS tiene_correo_electronico,
    persona.scholar_user_id AS id_google_scholar,
    persona.scholar_profile_url AS url_google_scholar,
    persona.scholar_user_id IS NOT NULL AS tiene_google_scholar,
    persona.researchgate,
    persona.researchgate IS NOT NULL AS tiene_researchgate,
    afiliacion.paragraph_id AS id_afiliacion_voc,
    afiliacion.paragraph_id IS NOT NULL AS tiene_afiliacion,
    afiliacion.afiliacion_esta_publicada AS afiliacion_publicada,
    afiliacion.esta_vigente_hoy AS afiliacion_vigente,
    afiliacion.fecha_inicio AS afiliacion_desde,
    afiliacion.fecha_fin AS afiliacion_hasta,
    institucion.institution_node_id AS id_institucion_voc,
    institucion.nombre_institucion AS institucion,
    institucion.abreviatura AS abreviatura_institucion,
    CASE
        WHEN institucion.institution_node_id IS NOT NULL
            THEN 'http://voc.sedici.unlp.edu.ar/node/'
                || institucion.institution_node_id
    END AS url_institucion_voc,
    institucion.esta_publicada AS institucion_publicada,
    institucion.pertenece_arbol_unlp AS pertenece_a_unlp,
    institucion.unidad_principal_unlp_nombre AS unidad_principal_unlp,
    institucion.jerarquia_unlp
FROM persona
LEFT JOIN afiliacion
    USING (person_node_id)
LEFT JOIN institucion
    USING (institution_node_id)
