{{ config(materialized='view') }}

-- 02. Universo recuperado en SEDICI mediante autoridad y coincidencia nominal.

{% set authority = var('sedici_authority') %}
{% set author_name_term = var('sedici_author_name_term') %}

WITH publicacion AS (
    SELECT *
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
    WHERE handle IS NOT NULL
),

autoria AS (
    SELECT
        author.item_id,
        STRING_AGG(
            DISTINCT author.author_name_raw,
            '; '
            ORDER BY author.author_name_raw
        ) AS autoria,
        COUNT(DISTINCT author.author_id)::integer AS cantidad_autores,
        COALESCE(
            BOOL_OR(author.authority = '{{ authority }}'),
            FALSE
        ) AS coincide_autoridad_voc,
        COALESCE(
            BOOL_OR(
                LOWER(author.author_name) ~
                    '(^|[^[:alpha:]]){{ author_name_term | lower }}([^[:alpha:]]|$)'
            ),
            FALSE
        ) AS coincide_texto_autor
    FROM {{ ref('brg_unlp_sedicidb_item_author') }} AS author
    INNER JOIN publicacion
        USING (item_id)
    WHERE author.author_role = 'creator_person'
    GROUP BY author.item_id
),

final AS (
    SELECT
        publicacion.handle,
        publicacion.dc_identifier_uri AS url_sedici,
        publicacion.dc_title AS titulo,
        autoria.autoria,
        COALESCE(autoria.cantidad_autores, 0) AS cantidad_autores,
        COALESCE(autoria.cantidad_autores = 1, FALSE) AS es_autoria_unica,
        autoria.coincide_autoridad_voc,
        autoria.coincide_texto_autor,
        CASE
            WHEN autoria.coincide_autoridad_voc
             AND autoria.coincide_texto_autor
                THEN 'Autoridad y texto'
            WHEN autoria.coincide_autoridad_voc
                THEN 'Solo autoridad'
            ELSE 'Solo texto'
        END AS criterio_identificacion_autor,
        publicacion.dc_type AS tipo_publicacion,
        publicacion.sedici_subtype AS subtipo_publicacion,
        publicacion.dc_date_issued AS fecha_publicacion,
        publicacion.publication_year AS anio_publicacion,
        publicacion.sedici_date_exposure AS fecha_exposicion,
        publicacion.dc_date_available AS fecha_disponible_en_sedici,
        CASE
            WHEN publicacion.withdrawn THEN 'Retirada'
            WHEN NOT publicacion.in_archive THEN 'No archivada'
            WHEN NOT publicacion.discoverable THEN 'No visible'
            ELSE 'Publicada y visible'
        END AS estado_sedici,
        'https://scholar.google.com/scholar?hl=es&as_epq='
            || REPLACE(publicacion.dc_title, ' ', '%20')
            AS url_busqueda_google_scholar
    FROM publicacion
    INNER JOIN autoria
        USING (item_id)
    WHERE autoria.coincide_autoridad_voc
       OR autoria.coincide_texto_autor
)

SELECT *
FROM final
ORDER BY titulo
