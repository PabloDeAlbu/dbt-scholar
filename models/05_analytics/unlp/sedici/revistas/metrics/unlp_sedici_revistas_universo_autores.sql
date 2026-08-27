{{ config(materialized='view') }}

WITH author_in_scope AS (
    SELECT
        id_autor AS author_id,
        MIN(nombre_autor) AS author_name,
        BOOL_OR(tiene_autoridad) AS has_authority_control,
        BOOL_OR(tiene_correspondencia_voc) AS has_voc_match,
        MAX(publicaciones_autor_en_sedici) AS sedici_publication_count,
        MAX(publicaciones_autor_en_revistas) AS journal_publication_count,
        MAX(articulos_autor_en_revistas) AS journal_article_count,
        COUNT(DISTINCT handle_revista)::bigint AS journal_count,
        MIN(anio_publicacion) AS first_publication_year,
        MAX(anio_publicacion) AS last_publication_year
    FROM {{ ref('unlp_sedici_revistas_dashboard') }}
    WHERE es_articulo IS TRUE
      AND id_autor IS NOT NULL
    GROUP BY id_autor
),

voc_identity AS (
    SELECT
        author_id,
        MIN(voc_person_node_id) AS voc_person_node_id
    FROM {{ ref('brg_unlp_sedici_revista_publicacion_autor') }}
    WHERE has_voc_match
    GROUP BY author_id
),

author_prepared AS (
    SELECT
        author.author_id,
        scope.author_name,
        author.author_name_normalized,
        CASE
            WHEN scope.has_authority_control THEN 'authority'
            ELSE 'normalized_name'
        END::text AS identity_basis,
        author.authority_uri,
        author.authority_host,
        scope.has_authority_control,
        voc.voc_person_node_id,
        scope.has_voc_match,
        author.observed_name_variant_count,
        scope.sedici_publication_count,
        scope.journal_publication_count,
        scope.journal_article_count,
        scope.journal_count,
        scope.first_publication_year,
        scope.last_publication_year
    FROM author_in_scope AS scope
    INNER JOIN {{ ref('dim_unlp_sedicidb_author') }} AS author
        USING (author_id)
    LEFT JOIN voc_identity AS voc
        USING (author_id)
),

final AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY author_name_normalized
        )::bigint AS normalized_name_identity_count
    FROM author_prepared
)

SELECT
    *,
    normalized_name_identity_count > 1 AS has_shared_normalized_name
FROM final
