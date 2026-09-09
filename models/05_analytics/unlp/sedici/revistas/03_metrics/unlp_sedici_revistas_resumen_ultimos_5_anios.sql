{{ config(materialized='view') }}

WITH periodo AS (
    SELECT
        (EXTRACT(YEAR FROM CURRENT_DATE)::integer - 4) AS anio_desde,
        EXTRACT(YEAR FROM CURRENT_DATE)::integer AS anio_hasta
),

base AS (
    SELECT dashboard.*
    FROM {{ ref('unlp_sedici_revistas_dashboard') }} AS dashboard
    CROSS JOIN periodo
    WHERE dashboard.anio_publicacion BETWEEN periodo.anio_desde AND periodo.anio_hasta
)

SELECT
    handle_revista,
    MIN(url_handle_revista) AS url_handle_revista,
    revista,
    MIN(estado_revista) AS estado_revista,
    MIN(periodo.anio_desde) AS anio_desde,
    MIN(periodo.anio_hasta) AS anio_hasta,
    COUNT(DISTINCT handle_publicacion)::bigint AS publicaciones,
    COUNT(DISTINCT id_autor)::bigint AS autores_identificados,
    COUNT(DISTINCT id_autor) FILTER (
        WHERE tiene_correspondencia_voc IS TRUE
    )::bigint AS autores_relacionados_voc,
    COUNT(DISTINCT id_autor) FILTER (
        WHERE tiene_afiliacion_unlp IS TRUE
    )::bigint AS autores_con_afiliacion_unlp_registrada,
    ROUND(
        100.0 * COUNT(DISTINCT id_autor) FILTER (
            WHERE tiene_afiliacion_unlp IS TRUE
        ) / NULLIF(COUNT(DISTINCT id_autor), 0),
        1
    ) AS proporcion_autores_con_afiliacion_unlp
FROM base
CROSS JOIN periodo
GROUP BY handle_revista, revista
