{{ config(materialized='view') }}

SELECT
    handle_revista,
    MIN(url_handle_revista) AS url_handle_revista,
    revista,
    MIN(estado_revista) AS estado_revista,
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
FROM {{ ref('unlp_sedici_revistas_dashboard') }}
GROUP BY handle_revista, revista
