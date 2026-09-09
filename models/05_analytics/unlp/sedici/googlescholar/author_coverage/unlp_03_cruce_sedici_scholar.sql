{{ config(materialized='view') }}

WITH sedici AS (
    SELECT
        *,
        REGEXP_REPLACE(
            TRANSLATE(LOWER(titulo), 'áéíóúüñ', 'aeiouun'),
            '[^a-z0-9]+',
            '',
            'g'
        ) AS titulo_normalizado
    FROM {{ ref('unlp_02_publicaciones_sedici_por_autor') }}
),

scholar AS (
    SELECT
        *,
        REGEXP_REPLACE(
            TRANSLATE(LOWER(titulo), 'áéíóúüñ', 'aeiouun'),
            '[^a-z0-9]+',
            '',
            'g'
        ) AS titulo_normalizado
    FROM {{ ref('unlp_01_publicaciones_scholar_por_perfil') }}
),

sedici_con_cardinalidad AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY titulo_normalizado, anio_publicacion
        ) AS cantidad_titulos_sedici
    FROM sedici
),

scholar_con_cardinalidad AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY titulo_normalizado, anio_publicacion
        ) AS cantidad_titulos_scholar
    FROM scholar
),

correspondencia AS (
    SELECT
        sedici.handle,
        COUNT(scholar.id_publicacion_scholar)::integer AS cantidad_coincidencias_scholar,
        MAX(scholar.cantidad_titulos_scholar)::integer AS cantidad_titulos_scholar,
        MAX(scholar.titulo) AS titulo_scholar,
        MAX(scholar.url_publicacion_scholar) AS url_publicacion_scholar,
        MAX(scholar.cantidad_citas) AS cantidad_citas
    FROM sedici_con_cardinalidad AS sedici
    LEFT JOIN scholar_con_cardinalidad AS scholar
        ON scholar.titulo_normalizado = sedici.titulo_normalizado
       AND scholar.anio_publicacion = sedici.anio_publicacion
    GROUP BY sedici.handle
),

final AS (
    SELECT
        sedici.handle,
        sedici.url_sedici,
        sedici.titulo AS titulo_sedici,
        sedici.autoria AS autoria_sedici,
        sedici.cantidad_autores,
        sedici.es_autoria_unica,
        sedici.coincide_autoridad_voc,
        sedici.coincide_texto_autor,
        sedici.criterio_identificacion_autor,
        sedici.tipo_publicacion,
        sedici.subtipo_publicacion,
        sedici.fecha_publicacion,
        sedici.anio_publicacion,
        sedici.fecha_exposicion,
        sedici.fecha_disponible_en_sedici,
        sedici.estado_sedici,
        CASE
            WHEN correspondencia.cantidad_coincidencias_scholar = 0
                THEN 'Sin coincidencia exacta'
            WHEN sedici.coincide_autoridad_voc
             AND sedici.cantidad_titulos_sedici = 1
             AND correspondencia.cantidad_titulos_scholar = 1
                THEN 'Confirmada'
            ELSE 'Requiere revisión'
        END AS estado_correspondencia,
        correspondencia.titulo_scholar,
        correspondencia.url_publicacion_scholar,
        correspondencia.cantidad_citas,
        sedici.url_busqueda_google_scholar
    FROM sedici_con_cardinalidad AS sedici
    INNER JOIN correspondencia
        USING (handle)
)

SELECT *
FROM final
ORDER BY estado_correspondencia, es_autoria_unica DESC, titulo_sedici
