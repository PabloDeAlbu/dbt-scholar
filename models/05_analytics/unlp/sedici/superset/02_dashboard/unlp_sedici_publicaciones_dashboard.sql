{{ config(materialized='view') }}

WITH publicacion AS (
    SELECT *
    FROM {{ ref('unlp_sedici_publicacion_digitalizacion') }}
)

SELECT
    handle,
    'https://hdl.handle.net/' || handle AS url_handle,
    'https://sedici.unlp.edu.ar/handle/' || handle AS url_publicacion,
    dc_title AS titulo,
    CASE
        WHEN CHAR_LENGTH(dc_title) > 50 THEN LEFT(dc_title, 50) || '...'
        ELSE dc_title
    END AS titulo_truncado,
    dc_type AS tipo_publicacion,
    sedici_subtype AS subtipo_publicacion,
    fue_digitalizada,
    evidencia_digitalizacion,
    dc_date_issued AS fecha_publicacion,
    publication_year AS anio_publicacion,
    dc_date_available AS fecha_disponibilidad_sedici,
    DATE_TRUNC('month', dc_date_available)::date AS mes_disponibilidad_sedici,
    EXTRACT(YEAR FROM dc_date_available)::int AS anio_disponibilidad_sedici,
    owning_root_community_title AS comunidad_raiz,
    owning_root_child_community_title AS comunidad_principal,
    owning_community_title AS comunidad,
    owning_collection_title AS coleccion,
    owning_community_path_titles AS ruta_en_sedici
FROM publicacion
