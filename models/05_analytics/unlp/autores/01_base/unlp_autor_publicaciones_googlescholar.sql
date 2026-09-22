WITH persona_referencia AS (
    SELECT * FROM {{ ref('seed_unlp_persona_referencia') }}
    WHERE id_google_scholar IS NOT NULL
),

final AS (
    SELECT
        persona.nombre AS persona,
        persona.id_google_scholar AS id_perfil_scholar,
        perfil.profile_name AS nombre_perfil,
        perfil.profile_url AS url_perfil_scholar,
        publicacion.scholar_publication_id AS id_publicacion_scholar,
        publicacion.title AS titulo,
        publicacion.publication_year AS anio_publicacion,
        publicacion.authors AS autores_declarados,
        publicacion.publication_details AS detalles_publicacion,
        publicacion.cited_by_count AS cantidad_citas,
        publicacion.position AS posicion_en_perfil,
        CASE WHEN publicacion.publication_url LIKE '/%'
            THEN 'https://scholar.google.com' || publicacion.publication_url
            ELSE publicacion.publication_url END AS url_publicacion_scholar,
        publicacion.googlescholar_cluster_id AS id_cluster_googlescholar,
        CASE WHEN publicacion.cluster_url LIKE '/%'
            THEN 'https://scholar.google.com' || publicacion.cluster_url
            ELSE publicacion.cluster_url END AS url_cluster_googlescholar,
        perfil.observed_on AS fecha_observacion,
        perfil.load_datetime AS fecha_carga
    FROM persona_referencia AS persona
    INNER JOIN {{ ref('dim_googlescholar_profile') }} AS perfil
        ON perfil.scholar_user_id = persona.id_google_scholar
    INNER JOIN {{ ref('fct_googlescholar_profile_publication') }} AS publicacion
        USING (scholar_user_id)
)

SELECT * FROM final
