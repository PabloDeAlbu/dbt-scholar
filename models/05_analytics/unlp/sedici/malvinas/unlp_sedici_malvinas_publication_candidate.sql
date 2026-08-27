WITH alternative_title_value AS (
    SELECT DISTINCT ON (item_id, LOWER(text_value))
        item_id,
        metadata_value_id,
        text_value,
        place
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dc.title.alternative'
      AND text_value IS NOT NULL
    ORDER BY
        item_id,
        LOWER(text_value),
        place NULLS LAST,
        metadata_value_id
),

alternative_title AS (
    SELECT
        item_id,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) AS titulos_alternativos
    FROM alternative_title_value
    GROUP BY item_id
)

SELECT
    'https://sedici.unlp.edu.ar/handle/' || publication.id AS url_sedici,
    CASE
        WHEN NULLIF(BTRIM(publication.subtitle), '') IS NULL
            THEN item.dc_title
        ELSE
            item.dc_title
            || ': '
            || REPLACE(publication.subtitle, '|', '; ')
    END AS titulo,
    alternative_title.titulos_alternativos,
    REPLACE(publication.author, '|', '; ') AS autoria,
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
INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS item
    ON item.item_id = solr.search_resource_id
LEFT JOIN alternative_title
    ON alternative_title.item_id = item.item_id
