SELECT
    'https://sedici.unlp.edu.ar/handle/' || publication.id AS url_sedici,
    REPLACE(publication.title, '|', '; ') AS titulo,
    REPLACE(publication.subtitle, '|', '; ') AS subtitulo,
    REPLACE(publication.author, '|', '; ') AS autorias,
    COALESCE(
        NULLIF(BTRIM(publication.subtype), ''),
        NULLIF(BTRIM(publication.type_raw), ''),
        NULLIF(BTRIM(publication.type_dedup), ''),
        NULLIF(BTRIM(publication.type), '')
    ) AS tipo_documento,
    publication.date AS fecha_publicacion,
    publication.publication_year AS anio_publicacion,
    publication.dc_date_available AS fecha_disponible_en_sedici,
    REPLACE(publication.subject, '|', '; ') AS temas,
    REPLACE(publication.description, '|', '; ') AS resumen,
    REPLACE(publication.doi, '|', '; ') AS doi,
    REPLACE(publication.isbn, '|', '; ') AS isbn,
    REPLACE(publication.issn, '|', '; ') AS issn,
    publication.owning_community_title AS comunidad,
    publication.owning_community_path_titles AS ruta_comunidades,
    publication.owning_collection_title AS coleccion
FROM {{ ref('seed_unlp_sedici_malvinas_solr_item_candidate') }} AS solr
INNER JOIN {{ ref('fct_unlp_sedicidb_dedup_publication') }} AS publication
    ON publication.id = solr.handle
