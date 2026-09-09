WITH publicacion AS (
    SELECT
        publicacion.scholar_user_id,
        COUNT(publicacion.scholar_publication_id)::integer AS cantidad_publicaciones,
        COUNT(*) FILTER (
            WHERE publicacion.publication_year BETWEEN 1000 AND EXTRACT(YEAR FROM CURRENT_DATE)::integer
        )::integer AS publicaciones_con_anio,
        MIN(publicacion.publication_year) FILTER (
            WHERE publicacion.publication_year BETWEEN 1000 AND EXTRACT(YEAR FROM CURRENT_DATE)::integer
        ) AS primer_anio_publicacion,
        MAX(publicacion.publication_year) FILTER (
            WHERE publicacion.publication_year BETWEEN 1000 AND EXTRACT(YEAR FROM CURRENT_DATE)::integer
        ) AS ultimo_anio_publicacion
    FROM {{ ref('fct_googlescholar_profile_publication') }} AS publicacion
    GROUP BY publicacion.scholar_user_id
),

final AS (
    SELECT
        perfil.scholar_user_id AS id_perfil_scholar,
        perfil.profile_name AS autor,
        perfil.profile_url AS url_perfil_scholar,
        perfil.orcid,
        perfil.affiliation AS afiliacion_declarada,
        perfil.organization_name AS organizacion_scholar,
        perfil.organization_url AS url_organizacion_scholar,
        perfil.verified_email AS email_verificado,
        NULLIF(
            REPLACE(REPLACE(REPLACE(perfil.interests, '["', ''), '"]', ''), '", "', ' | '),
            '[]'
        )
            AS intereses_declarados,
        COALESCE(publicacion.cantidad_publicaciones, 0) AS cantidad_publicaciones,
        COALESCE(publicacion.publicaciones_con_anio, 0) AS publicaciones_con_anio,
        publicacion.primer_anio_publicacion,
        publicacion.ultimo_anio_publicacion,
        perfil.cited_by_count_total AS cantidad_citas,
        perfil.h_index_total AS indice_h,
        perfil.i10_index_total AS indice_i10,
        perfil.observed_on AS fecha_observacion,
        perfil.load_datetime AS fecha_carga
    FROM {{ ref('dim_googlescholar_profile') }} AS perfil
    LEFT JOIN publicacion USING (scholar_user_id)
)

SELECT * FROM final ORDER BY autor
