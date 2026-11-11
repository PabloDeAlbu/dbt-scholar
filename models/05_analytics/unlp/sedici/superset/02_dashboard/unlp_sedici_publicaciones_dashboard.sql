{{ config(materialized='view') }}

-- ¿Qué datos descriptivos y de digitalización aporta cada publicación?
WITH publicacion AS (
    SELECT
        item_id,
        handle,
        dc_title,
        dc_type,
        sedici_subtype,
        fue_digitalizada,
        evidencia_digitalizacion,
        dc_date_issued,
        publication_year,
        dc_date_accessioned,
        dc_date_available,
        owning_root_child_community_title,
        owning_community_title,
        owning_collection_title,
        owning_community_path_titles
    FROM {{ ref('unlp_sedici_publicacion_digitalizacion') }}
),

-- ¿Qué cobertura en fuentes externas se identificó para cada publicación?
cobertura_externa AS (
    SELECT
        item_id,
        en_openalex,
        correspondencia_openalex,
        en_openaire,
        correspondencia_openaire,
        confirmacion_entre_fuentes_externas
    FROM {{ ref('unlp_sedici_publicacion_cobertura_externa') }}
),

-- ¿Qué ubicación y clasificación por unidad académica aporta cada publicación?
unidad_academica AS (
    SELECT
        item_id,
        comunidad_raiz,
        comunidad_nivel_2,
        es_unidad_academica,
        unidad_academica,
        unidad_academica_evidencia
    FROM {{ ref('unlp_sedici_publicacion_unidad_academica') }}
),

-- ¿Qué datos se presentan para explorar las publicaciones en Superset?
final AS (
    SELECT
        publicacion.handle,
        'https://hdl.handle.net/' || publicacion.handle AS url_handle,
        'https://sedici.unlp.edu.ar/handle/' || publicacion.handle AS url_publicacion,
        publicacion.dc_title AS titulo,
        CASE
            WHEN CHAR_LENGTH(publicacion.dc_title) > 50
                THEN LEFT(publicacion.dc_title, 50) || '...'
            ELSE publicacion.dc_title
        END AS titulo_truncado,
        publicacion.dc_type AS tipo_publicacion,
        publicacion.sedici_subtype AS subtipo_publicacion,
        publicacion.fue_digitalizada,
        publicacion.evidencia_digitalizacion,
        publicacion.dc_date_issued AS fecha_publicacion,
        publicacion.publication_year AS anio_publicacion,
        publicacion.dc_date_accessioned AS fecha_ingreso_sedici,
        DATE_TRUNC('month', publicacion.dc_date_accessioned)::date AS mes_ingreso_sedici,
        EXTRACT(YEAR FROM publicacion.dc_date_accessioned)::int AS anio_ingreso_sedici,
        publicacion.dc_date_available AS fecha_disponibilidad_sedici,
        DATE_TRUNC('month', publicacion.dc_date_available)::date AS mes_disponibilidad_sedici,
        EXTRACT(YEAR FROM publicacion.dc_date_available)::int AS anio_disponibilidad_sedici,
        unidad_academica.comunidad_raiz,
        unidad_academica.comunidad_nivel_2,
        publicacion.owning_root_child_community_title AS comunidad_principal,
        unidad_academica.es_unidad_academica,
        unidad_academica.unidad_academica,
        unidad_academica.unidad_academica_evidencia,
        publicacion.owning_community_title AS comunidad,
        publicacion.owning_collection_title AS coleccion,
        publicacion.owning_community_path_titles AS ruta_en_sedici,
        cobertura_externa.en_openalex,
        cobertura_externa.correspondencia_openalex,
        cobertura_externa.en_openaire,
        cobertura_externa.correspondencia_openaire,
        cobertura_externa.confirmacion_entre_fuentes_externas
    FROM publicacion
    INNER JOIN cobertura_externa
        USING (item_id)
    INNER JOIN unidad_academica
        USING (item_id)
)

SELECT *
FROM final
