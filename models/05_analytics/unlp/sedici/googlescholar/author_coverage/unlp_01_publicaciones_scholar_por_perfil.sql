{{ config(materialized='view') }}

-- 01. Universo observado en el perfil público seleccionado de Scholar.

{% set profile_user_id = var('googlescholar_profile_user_id') %}

WITH perfil AS (
    SELECT
        scholar_user_id,
        profile_name,
        profile_url,
        observed_on
    FROM {{ ref('ldg_googlescholar_profile') }}
    WHERE scholar_user_id = '{{ profile_user_id }}'
),

publicacion AS (
    SELECT
        scholar_user_id,
        scholar_publication_id,
        title,
        authors,
        publication_details,
        publication_year,
        cited_by_count,
        publication_url,
        cluster_url,
        observed_on
    FROM {{ ref('ldg_googlescholar_profile_publication') }}
    WHERE scholar_user_id = '{{ profile_user_id }}'
),

final AS (
    SELECT
        publicacion.title AS titulo,
        publicacion.authors AS autoria,
        publicacion.publication_details,
        publicacion.publication_year AS anio_publicacion,
        COALESCE(publicacion.cited_by_count, 0) AS cantidad_citas,
        publicacion.publication_url AS url_publicacion_scholar,
        CASE
            WHEN publicacion.cluster_url IS NOT NULL THEN
                'https://scholar.google.com/scholar?cluster='
                || SUBSTRING(publicacion.cluster_url FROM '(?:cluster|cites)=([0-9]+)')
                || '&hl=es&as_sdt=2005&sciodt=0,5'
        END AS url_cluster_scholar,
        perfil.profile_name AS nombre_perfil_scholar,
        perfil.profile_url AS url_perfil_scholar,
        publicacion.observed_on AS fecha_observacion,
        publicacion.scholar_publication_id AS id_publicacion_scholar
    FROM publicacion
    INNER JOIN perfil
        ON perfil.scholar_user_id = publicacion.scholar_user_id
       AND perfil.observed_on = publicacion.observed_on
)

SELECT *
FROM final
ORDER BY titulo
