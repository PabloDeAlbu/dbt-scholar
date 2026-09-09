{{ config(materialized='view') }}

WITH vocsedici AS (
    SELECT
        item_id,
        sedicidb_author_id,
        has_voc_match,
        voc_person_url,
        affiliation_status,
        unlp_institution_count,
        external_institution_count
    FROM {{ ref('fct_unlp_sedici_revista_autoria_afiliacion_vocsedici') }}
),

publication_voc_status AS (
    SELECT
        item_id,
        BOOL_OR(has_voc_match) AS has_voc_author
    FROM {{ ref('fct_unlp_sedici_revista_autoria_afiliacion_vocsedici') }}
    GROUP BY item_id
)

SELECT
    item.handle AS handle_publicacion,
    'https://hdl.handle.net/' || item.handle AS url_handle_publicacion,
    item.item_url AS url_publicacion,
    item.item_title AS titulo_publicacion,
    item.sedici_subtype AS tipo_publicacion,
    item.publication_date AS fecha_publicacion,
    item.publication_year AS anio_publicacion,
    item.sedici_community_title AS revista,
    item.sedici_community_handle AS handle_revista,
    CASE
        WHEN item.sedici_community_handle IS NOT NULL
            THEN 'https://hdl.handle.net/' || item.sedici_community_handle
    END AS url_handle_revista,
    item.is_closed AS es_cerrada,
    CASE
        WHEN item.is_closed THEN 'Cerrada'
        ELSE 'Abierta'
    END AS estado_revista,
    item.owning_community_path_titles AS ruta_en_sedici,
    CASE
        WHEN COALESCE(publication_voc_status.has_voc_author, FALSE)
            THEN 'Con autor en VOC SEDICI'
        ELSE 'Sin autor en VOC SEDICI'
    END AS correspondencia_voc_publicacion,
    authorship.author_name AS nombre_autor,
    authorship.sedicidb_author_id IS NOT NULL AS tiene_autor_reconocido,
    authorship.has_authority_control AS tiene_autoridad,
    vocsedici.has_voc_match AS tiene_correspondencia_voc,
    vocsedici.voc_person_url AS url_autor_voc,
    CASE vocsedici.affiliation_status
        WHEN 'unlp' THEN 'Afiliación UNLP registrada'
        WHEN 'external' THEN 'Otra afiliación registrada'
        WHEN 'unknown' THEN 'Sin afiliación aplicable registrada'
    END AS situacion_afiliacion,
    CASE
        WHEN vocsedici.affiliation_status = 'unlp' THEN TRUE
        WHEN vocsedici.affiliation_status = 'external' THEN FALSE
    END AS tiene_afiliacion_unlp,
    vocsedici.unlp_institution_count AS afiliaciones_unlp,
    vocsedici.external_institution_count AS otras_afiliaciones,
    authorship.sedicidb_author_id AS id_autor,
    CASE
        WHEN authorship.sedicidb_author_id IS NOT NULL
            THEN item.handle || '|' || authorship.sedicidb_author_id
    END AS id_autoria
FROM {{ ref('fct_unlp_sedici_revista_publicacion') }} AS item
LEFT JOIN {{ ref('brg_unlp_sedici_revista_publicacion_autor') }} AS authorship
    USING (item_id)
LEFT JOIN vocsedici
    USING (item_id, sedicidb_author_id)
LEFT JOIN publication_voc_status
    USING (item_id)
