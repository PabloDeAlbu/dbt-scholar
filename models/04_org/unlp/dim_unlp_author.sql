{{ config(materialized='table') }}

-- ¿Qué autoridades y nombres de autor aporta SEDICIDB?
WITH sedicidb_author AS (
    SELECT
        authority,
        preferred_name AS sedicidb_preferred_name,
        names AS sedicidb_names
    FROM {{ ref('dim_unlp_sedicidb_author') }}
),

-- ¿Qué personas e identificadores aporta el estado actual de VOC SEDICI?
voc_person AS (
    SELECT
        person_node_id,
        nombre_completo AS voc_name,
        persona_url,
        orcid,
        scholar_user_id,
        scholar_profile_url
    FROM {{ ref('dim_vocsedici_persona') }}
),

-- ¿Qué autoridades observadas en SEDICIDB se relacionan con personas de VOC SEDICI?
final AS (
    SELECT
        author.authority,
        author.sedicidb_preferred_name,
        author.sedicidb_names,
        voc.person_node_id AS voc_person_node_id,
        voc.voc_name,
        voc.person_node_id IS NOT NULL AS has_voc_match,
        voc.persona_url AS voc_person_url,
        voc.orcid,
        voc.scholar_user_id,
        voc.scholar_profile_url
    FROM sedicidb_author AS author
    LEFT JOIN voc_person AS voc
        ON voc.persona_url = author.authority
)

SELECT *
FROM final
