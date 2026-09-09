{{ config(materialized='table') }}

WITH author AS (
    SELECT *
    FROM {{ ref('dim_unlp_sedicidb_author') }}
),

voc_person AS (
    SELECT
        person_node_id,
        nombre_completo,
        persona_url,
        orcid,
        scholar_user_id,
        scholar_profile_url
    FROM {{ ref('dim_vocsedici_persona') }}
),

final AS (
    SELECT
        author.author_id,
        author.author_bk,
        author.author_name_preferred,
        author.author_name_normalized,
        author.author_type,
        author.author_roles,
        author.authority,
        author.has_authority_control,
        CASE
            WHEN author.has_authority_control THEN 'autoridad'
            ELSE 'nombre_normalizado'
        END::text AS author_identity_basis,
        voc.person_node_id AS voc_person_node_id,
        voc.person_node_id IS NOT NULL AS has_voc_match,
        voc.persona_url AS voc_person_url,
        voc.orcid,
        voc.scholar_user_id,
        voc.scholar_profile_url,
        author.observation_count,
        author.item_count,
        author.observed_name_variant_count
    FROM author
    LEFT JOIN voc_person AS voc
        ON voc.persona_url = author.authority
)

SELECT *
FROM final
