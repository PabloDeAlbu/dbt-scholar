WITH final AS (
    SELECT
        perfil.scholar_user_id AS id_perfil_scholar,
        perfil.profile_name AS autor,
        perfil.profile_url AS url_perfil_scholar,
        perfil.affiliation AS afiliacion_declarada,
        NULLIF(
            REPLACE(REPLACE(REPLACE(perfil.interests, '["', ''), '"]', ''), '", "', ' | '),
            '[]'
        )
            AS intereses_declarados,
        publicacion.scholar_publication_id AS id_publicacion_scholar,
        publicacion.title AS titulo,
        publicacion.authors AS autores_declarados,
        publicacion.publication_details AS detalles_publicacion,
        CASE
            WHEN publicacion.publication_year BETWEEN 1000
                AND EXTRACT(YEAR FROM CURRENT_DATE)::integer
                THEN publicacion.publication_year
        END AS anio_publicacion,
        COALESCE(publicacion.cited_by_count, 0) AS cantidad_citas,
        publicacion.position AS posicion_en_perfil,
        CASE
            WHEN publicacion.publication_url LIKE '/%'
                THEN 'https://scholar.google.com' || publicacion.publication_url
            ELSE publicacion.publication_url
        END AS url_publicacion_scholar,
        publicacion.googlescholar_cluster_id AS id_cluster_googlescholar,
        CASE
            WHEN publicacion.cluster_url LIKE '/%'
                THEN 'https://scholar.google.com' || publicacion.cluster_url
            ELSE publicacion.cluster_url
        END AS url_cluster_googlescholar,
        perfil.observed_on AS fecha_observacion,
        perfil.load_datetime AS fecha_carga
    FROM {{ ref('dim_googlescholar_profile') }} AS perfil
    INNER JOIN {{ ref('fct_googlescholar_profile_publication') }} AS publicacion
        USING (scholar_user_id)
)

SELECT * FROM final ORDER BY autor, posicion_en_perfil
