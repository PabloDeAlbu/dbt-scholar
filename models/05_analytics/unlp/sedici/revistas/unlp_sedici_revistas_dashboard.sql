{{ config(materialized='view') }}

WITH sedici_author_stats AS (
    SELECT
        authorship.author_id,
        COUNT(DISTINCT publication.item_id)::bigint
            AS sedici_publication_count
    FROM {{ ref('brg_unlp_sedicidb_item_author') }} AS authorship
    INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS publication
        USING (item_id)
    WHERE authorship.author_role = 'creator_person'
      AND authorship.author_type = 'person'
      AND publication.in_archive IS TRUE
      AND publication.withdrawn IS FALSE
      AND publication.discoverable IS TRUE
      AND publication.handle IS NOT NULL
    GROUP BY authorship.author_id
),

journal_author_stats AS (
    SELECT
        author_id,
        COUNT(DISTINCT item_id)::bigint AS journal_publication_count,
        COUNT(DISTINCT item_id) FILTER (
            WHERE is_article
        )::bigint AS journal_article_count
    FROM {{ ref('brg_unlp_sedici_revista_publicacion_autor') }}
    GROUP BY author_id
)

SELECT
    item.handle AS handle_publicacion,
    'https://hdl.handle.net/' || item.handle AS url_handle_publicacion,
    item.item_url AS url_publicacion,
    item.item_title AS titulo_publicacion,
    item.sedici_subtype AS tipo_publicacion,
    item.publication_date AS fecha_publicacion,
    item.publication_year AS anio_publicacion,
    item.is_article AS es_articulo,
    item.journal_title AS revista,
    item.journal_handle AS handle_revista,
    CASE
        WHEN item.journal_handle IS NOT NULL
            THEN 'https://hdl.handle.net/' || item.journal_handle
    END AS url_handle_revista,
    item.is_closed AS es_cerrada,
    item.owning_community_path_titles AS ruta_en_sedici,
    authorship.author_name AS nombre_autor,
    authorship.author_id IS NOT NULL AS tiene_autor_reconocido,
    COALESCE(authorship.has_authority_control, FALSE) AS tiene_autoridad,
    COALESCE(authorship.has_voc_match, FALSE) AS tiene_correspondencia_voc,
    CASE
        WHEN authorship.voc_person_node_id IS NOT NULL
            THEN 'http://voc.sedici.unlp.edu.ar/node/'
                || authorship.voc_person_node_id
    END AS url_autor_voc,
    sedici.sedici_publication_count AS publicaciones_autor_en_sedici,
    journal.journal_publication_count AS publicaciones_autor_en_revistas,
    journal.journal_article_count AS articulos_autor_en_revistas,
    authorship.author_id AS id_autor,
    CASE
        WHEN authorship.author_id IS NOT NULL
            THEN item.handle || '|' || authorship.author_id
    END AS id_autoria,
    CASE
        WHEN authorship.author_id IS NULL THEN NULL
        WHEN authorship.has_authority_control THEN 'autoridad'
        ELSE 'nombre_normalizado'
    END::text AS base_identidad_autor
FROM {{ ref('fct_unlp_sedici_revista_publicacion') }} AS item
LEFT JOIN {{ ref('brg_unlp_sedici_revista_publicacion_autor') }} AS authorship
    USING (item_id)
LEFT JOIN sedici_author_stats AS sedici
    USING (author_id)
LEFT JOIN journal_author_stats AS journal
    USING (author_id)
