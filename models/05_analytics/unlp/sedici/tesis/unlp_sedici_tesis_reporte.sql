{{ config(materialized='view') }}

WITH tesis AS (
    SELECT *
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
    WHERE dc_type = 'Tesis'
      AND in_archive IS TRUE
      AND withdrawn IS FALSE
      AND discoverable IS TRUE
      AND handle IS NOT NULL
      AND owning_root_community_title = 'Unidades académicas'
),

metadata_value AS (
    SELECT DISTINCT ON (
        metadata.item_id,
        metadata.metadatafield_fullname,
        LOWER(metadata.text_value)
    )
        metadata.item_id,
        metadata.metadata_value_id,
        metadata.metadatafield_fullname,
        metadata.text_value,
        metadata.place
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }} AS metadata
    INNER JOIN tesis
        USING (item_id)
    WHERE metadata.metadatafield_fullname IN (
        'sedici.title.subtitle',
        'sedici.creator.person',
        'thesis.degree.name',
        'thesis.degree.grantor',
        'sedici.contributor.director',
        'sedici.contributor.codirector',
        'sedici.contributor.juror',
        'dc.language',
        'dc.subject',
        'sedici.subject.materias',
        'sedici.rights.license',
        'sedici.rights.uri'
    )
      AND metadata.text_value IS NOT NULL
    ORDER BY
        metadata.item_id,
        metadata.metadatafield_fullname,
        LOWER(metadata.text_value),
        metadata.metadata_value_id DESC
),

metadata_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.title.subtitle'
        ) AS subtitulo,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.creator.person'
        ) AS autoria,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'thesis.degree.name'
        ) AS grado,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'thesis.degree.grantor'
        ) AS institucion_otorgante,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.contributor.director'
        ) AS direccion,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.contributor.codirector'
        ) AS codireccion,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.contributor.juror'
        ) AS jurado,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'dc.language'
        ) AS idioma,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.rights.license'
        ) AS licencia,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) FILTER (
            WHERE metadatafield_fullname = 'sedici.rights.uri'
        ) AS url_licencia
    FROM metadata_value
    GROUP BY item_id
),

subject_value AS (
    SELECT DISTINCT ON (item_id, LOWER(text_value))
        item_id,
        metadata_value_id,
        text_value,
        place
    FROM metadata_value
    WHERE metadatafield_fullname IN (
        'dc.subject',
        'sedici.subject.materias'
    )
    ORDER BY
        item_id,
        LOWER(text_value),
        metadata_value_id DESC
),

subject_agg AS (
    SELECT
        item_id,
        STRING_AGG(
            REPLACE(text_value, '|', '; '),
            '; ' ORDER BY place NULLS LAST, metadata_value_id
        ) AS temas
    FROM subject_value
    GROUP BY item_id
),

reporte AS (
SELECT
    tesis.dc_identifier_uri AS url_sedici,
    CASE
        WHEN NULLIF(BTRIM(metadata.subtitulo), '') IS NULL
            THEN tesis.dc_title
        ELSE tesis.dc_title || ': ' || metadata.subtitulo
    END AS titulo,
    metadata.autoria,
    CASE tesis.sedici_subtype
        WHEN 'Tesis de maestria' THEN 'Tesis de maestría'
        WHEN 'Trabajo de especializacion' THEN 'Trabajo de especialización'
        ELSE tesis.sedici_subtype
    END AS tipo_tesis,
    metadata.grado,
    metadata.institucion_otorgante,
    metadata.direccion,
    metadata.codireccion,
    metadata.jurado,
    tesis.dc_date_issued AS fecha_publicacion,
    tesis.publication_year AS anio_publicacion,
    tesis.sedici_date_exposure AS fecha_exposicion,
    tesis.dc_date_available AS fecha_disponible_en_sedici,
    metadata.idioma,
    subject.temas,
    metadata.licencia,
    metadata.url_licencia,
    SPLIT_PART(
        tesis.owning_community_path_titles,
        ' > ',
        2
    ) AS unidad_academica,
    tesis.owning_community_title AS comunidad,
    tesis.owning_community_path_titles AS ruta_comunidades,
    tesis.owning_collection_title AS coleccion
FROM tesis
LEFT JOIN metadata_agg AS metadata
    USING (item_id)
LEFT JOIN subject_agg AS subject
    USING (item_id)
)

SELECT
    url_sedici,
    titulo,
    CASE
        WHEN CHAR_LENGTH(titulo) > 50
            THEN RTRIM(LEFT(titulo, 47)) || '...'
        ELSE titulo
    END AS titulo_truncado,
    autoria,
    tipo_tesis,
    grado,
    institucion_otorgante,
    direccion,
    codireccion,
    jurado,
    fecha_publicacion,
    anio_publicacion,
    fecha_exposicion,
    fecha_disponible_en_sedici,
    idioma,
    temas,
    licencia,
    url_licencia,
    unidad_academica,
    comunidad,
    ruta_comunidades,
    coleccion
FROM reporte
