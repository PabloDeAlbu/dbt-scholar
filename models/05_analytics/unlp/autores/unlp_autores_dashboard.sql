-- ¿Qué datos de identidad e identificadores aporta cada autoridad de autor?
WITH autor AS (
    SELECT
        authority,
        sedicidb_preferred_name,
        sedicidb_names,
        voc_name,
        has_voc_match,
        voc_person_url,
        orcid,
        scholar_user_id,
        scholar_profile_url
    FROM {{ ref('dim_unlp_author') }}
),

-- ¿Qué afiliaciones registradas resume cada autoridad de autor?
afiliacion AS (
    SELECT
        authority,
        COUNT(DISTINCT affiliation_id) AS cantidad_afiliaciones,
        BOOL_OR(is_unlp_affiliation) AS tiene_afiliacion_unlp,
        BOOL_OR(
            is_unlp_affiliation
            AND (valid_from IS NULL OR valid_from <= CURRENT_DATE)
            AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
        ) AS tiene_afiliacion_unlp_vigente,
        STRING_AGG(
            DISTINCT main_unlp_unit_name,
            ' | ' ORDER BY main_unlp_unit_name
        ) FILTER (
            WHERE is_unlp_affiliation
              AND main_unlp_unit_name IS NOT NULL
        ) AS unidades_principales_unlp
    FROM {{ ref('fct_unlp_author_affiliation') }}
    GROUP BY authority
),

-- ¿Qué datos se presentan para explorar los autores y sus afiliaciones?
final AS (
    SELECT
        autor.authority AS id_autor,
        autor.sedicidb_preferred_name AS autor,
        autor.sedicidb_names AS nombres_sedici,
        autor.voc_name AS nombre_voc,
        autor.has_voc_match AS tiene_correspondencia_voc,
        autor.voc_person_url AS url_persona_voc,
        autor.orcid,
        autor.orcid IS NOT NULL AS tiene_orcid,
        autor.scholar_user_id AS id_perfil_scholar,
        autor.scholar_profile_url AS url_perfil_scholar,
        autor.scholar_user_id IS NOT NULL AS tiene_perfil_scholar,
        COALESCE(afiliacion.cantidad_afiliaciones, 0) AS cantidad_afiliaciones,
        afiliacion.cantidad_afiliaciones IS NOT NULL AS tiene_afiliacion_registrada,
        COALESCE(afiliacion.tiene_afiliacion_unlp, FALSE) AS tiene_afiliacion_unlp,
        COALESCE(
            afiliacion.tiene_afiliacion_unlp_vigente,
            FALSE
        ) AS tiene_afiliacion_unlp_vigente,
        afiliacion.unidades_principales_unlp,
        'sedicidb'::text AS fuente_identidad
    FROM autor
    LEFT JOIN afiliacion
        USING (authority)
)

SELECT *
FROM final
